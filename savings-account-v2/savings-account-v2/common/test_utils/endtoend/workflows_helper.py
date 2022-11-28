# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# standard lib
import json
import logging
import os
import random
import re
import string
import uuid
from datetime import datetime, timedelta
from time import sleep
from typing import Any, Dict, Optional, List

# third party
import yaml
from jsonpath_ng import parse
from jsonpath_ng.jsonpath import JSONPath

# common
from common.test_utils.performance.data_objects.data_objects import (
    ExpectedWorklowInstantiation,
)
from common.test_utils.endtoend.kafka_helper import (
    kafka_only_helper,
    wait_for_messages,
)
import common.test_utils.endtoend.workflows_api_helper as workflows_api
import common.test_utils.endtoend as endtoend
from common.test_utils.endtoend.helper import send_request

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Default timeout value used when waiting for Workflows, can be overridden by individual tests
DEFAULT_TIMEOUT_SECS = 30
# As a workflow definition is just parsed YAML it could technically be Any
WorkflowDefinition = Any

CREATE_WORKFLOW_INSTANCE_TOPIC = (
    "vault.api.v1.workflows.workflow_instance.create.requests"
)


class WorkflowError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class WorkflowStuckError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class ChildWorkflowError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def get_state(wf):
    return wf["current_state_name"]


def reload_workflow(wf_id):
    body = {"ids": wf_id, "view": "WORKFLOW_INSTANCE_VIEW_INCLUDE_ADDITIONAL_DETAILS"}
    resp = send_request("get", "/v1/workflow-instances:batchGet", params=body)
    return resp["workflow_instances"][wf_id]


def is_instance_stuck(
    instance_id: str,
    transition_timeout: int = DEFAULT_TIMEOUT_SECS,
    latest_state: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Determines if an instance is currently stuck by
     - ensuring it's not completed (successfully or not)
     - ensuring it's not in technical_error state
     - comparing the time spent in its latest state to a configurable timeout. If a state has
     instantiated a child workflow, the timeout is ignored and a recursive call is made to the
     child workflow instead.
    :param instance_id: the id of the instance to check
    :param transition_timeout: max number of seconds before we expect a transition to occur.
    :param latest_state: the latest workflow state. If not provided this will be fetched via API
    :return: True if the instance is stuck, False otherwise
    """

    if not latest_state:
        latest_state = workflows_api.get_workflow_instance_states(
            instance_id,
            workflows_api.WorkflowStateOrderBy.ORDER_BY_TIMESTAMP_DESC,
            result_limit=1,
        )[0]

    # We assume that a workflow in technical_error is stuck. Perhaps this'll need revisiting if we
    # start overriding the implicit technical_error state?
    if latest_state["state_name"] == workflows_api.STATE_TECHNICAL_ERROR:
        return True

    # We consider a workflow to be stuck if it hasn't transitioned in transition_timeout seconds.
    try:
        latest_state_ts = datetime.strptime(
            latest_state["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )
    except ValueError:
        latest_state_ts = datetime.strptime(
            latest_state["timestamp"], "%Y-%m-%dT%H:%M:%SZ"
        )
    # Both naive datetimes representing UTC time
    if latest_state_ts + timedelta(seconds=transition_timeout) < datetime.utcnow():
        # parent instance only considered stuck if the child is also stuck
        log.info("checking children")
        try:
            child_workflow_id = get_child_workflow_id(
                instance_id, latest_state["state_name"], wait_for_parent_state=False
            )
            return is_instance_stuck(child_workflow_id)
        except ChildWorkflowError:
            # No child workflow can be found, so parent is genuinely stuck
            return True

    return False


def wait_for_state(
    wf_id: str,
    state_name: str,
    starting_state_id: str = "",
    transition_timeout: int = DEFAULT_TIMEOUT_SECS,
    overall_timeout: int = 120,
) -> Dict[str, Any]:
    """
    Wait for a workflow to progress to a state, allowing transition_timeout seconds for the
    workflow to make any progress at all. Times out after overall_timeout seconds irrespective of
    any progress made.
    :param wf_id: the workflow instance id for the instance we're expecting to progress
    :param state_name: the state we're expecting the workflow to progress to
    :param starting_state_id: if populated, any states up to and including the starting state are
    ignored. This ensures an actual transition occurs before returning
    :param transition_timeout: time in seconds before we consider workflow to be stuck. Reset
    every time one or more state transitions are detected
    :param overall_timeout: time in seconds before we consider workflow to be stuck. Never reset
    :return: the state that was waited for
    """

    start_time = datetime.utcnow()

    while datetime.utcnow() - timedelta(seconds=overall_timeout) < start_time:
        latest_state = workflows_api.get_workflow_instance_states(
            wf_id,
            workflows_api.WorkflowStateOrderBy.ORDER_BY_TIMESTAMP_DESC,
            result_limit=1,
        )[0]
        if (
            latest_state["state_name"] == state_name
            and latest_state["id"] != starting_state_id
        ):
            return latest_state
        elif is_instance_stuck(wf_id, transition_timeout, latest_state):
            raise WorkflowStuckError(
                f"{datetime.utcnow()} - "
                f'Workflow {wf_id} stuck in state {latest_state["state_name"]}'
                f' since {latest_state["timestamp"]}. Started checks at {start_time}'
            )
        sleep(0.5)
    raise WorkflowStuckError(
        f"{datetime.utcnow()} - "
        f"Workflow {wf_id} failed to reach state {state_name} after"
        f" overall timeout of {overall_timeout} seconds"
    )


def get_current_workflow_state(instance_id) -> Dict[str, Any]:
    """
    Retrieves the current workflow state
    :param instance_id: the workflow instance id
    :return: the current workflow state
    """

    return workflows_api.get_workflow_instance_states(instance_id, result_limit=1)[0]


def get_state_local_context(wf_id, state_name) -> Dict[str, str]:
    """
    Get the local context object for a workflow state
    :param workflow_instance_id: workflow id
    :param limit limit the result set
    :return: dictionary of local context keys and values
    """

    workflow_instance_events = workflows_api.get_workflow_instance_events(wf_id)
    for event in workflow_instance_events:
        if event["to_state_name"] == state_name:
            return event["context"]
    return {}


def get_global_context(wf_id):
    wf = reload_workflow(wf_id)
    return wf["global_state"]


def get_child_workflow_id(
    parent_instance_id: str,
    parent_state_name: str,
    wait_for_parent_state: bool = True,
    existing_instantiated_child_workflows: List[str] = None,
) -> str:
    """
    Retrieve the child workflow id for a child workflow started in a specific parent workflow state.
    This is not guaranteed to retrieve the correct child workflow if more than one have been
    triggered from the same state.
    :param parent_instance_id: the parent workflow instance id to check for a child
    :param parent_state_name: name of the parent state that triggers the workflow
    :param wait_for_parent_state: if False it is assumed that the parent workflow is already in the
    :param existing_instantiated_child_workflows: List of already instantiated child workflow ids
    use if multiple instances of the same child workflow are instantiated
    given state
    :return: the child workflow id
    """

    if wait_for_parent_state:
        wait_for_state(parent_instance_id, parent_state_name)
    parent_instance = workflows_api.batch_get_workflow_instances([parent_instance_id])[
        parent_instance_id
    ]

    # TODO: because we're likely to call this method a lot, consider caching the results
    wf_definition_id = parent_instance["workflow_definition_id"]
    wf_definition_version_id = parent_instance["workflow_definition_version_id"]
    wf_version = workflows_api.get_workflow_definition_version(wf_definition_version_id)
    parent_state_definition = next(
        iter(
            [
                state
                for state in wf_version["states"]
                if state["name"] == parent_state_name
            ]
        )
    )
    if not parent_state_definition["spawns_children"]:
        raise ChildWorkflowError(
            f"{datetime.utcnow()} - "
            f"Cannot get child workflow id for instance {parent_instance_id}. Workflow "
            f"{wf_definition_id} doesn't instantiate a child workflow in state {parent_state_name}"
        )

    # we may need to expand this if a workflow can loop and trigger the same workflow multiple times
    child_workflow_definition_version_ids = parent_state_definition[
        "child_workflow_definition_version_ids"
    ]
    child_workflow = endtoend.helper.retry_call(
        func=workflows_api.get_workflow_instances,
        f_kwargs=dict(
            workflow_definition_version_id=child_workflow_definition_version_ids[0],
            parent_ids=[parent_instance_id],
        ),
        expected_result=True,
        result_wrapper=lambda x: len(x) > 0,
        failure_message=f"Could not get child workflow id for parent workflow id"
        f" {parent_instance_id} and definition version id"
        f" {child_workflow_definition_version_ids[0]}",
    )

    if existing_instantiated_child_workflows:
        return [
            workflow["id"]
            for workflow in child_workflow
            if workflow["id"] not in existing_instantiated_child_workflows
        ][0]

    return child_workflow[0]["id"]


def send_event(
    wf_id: str,
    event_state: str,
    event_name: str,
    context: Optional[Dict[str, str]] = None,
    current_state_id: str = "",
):
    """
    Wait for a workflow to be in the desired state and then send an event
    :param wf_id: the workflow instance id
    :param event_state: the state the workflow instance must be in before sending the event
    :param event_name: the name of the event to send
    :param context: the context to attach to the event
    :param current_state_id: an optional current state id to guarantee we wait for a transition in
    case the state to wait for can be repeated
    :return: the id of the state the event was sent to
    """
    state_id = wait_for_state(wf_id, event_state, starting_state_id=current_state_id)[
        "id"
    ]

    post_body = {
        "request_id": uuid.uuid4().hex,
        "workflow_instance_event": {
            "id": uuid.uuid4().hex,
            "workflow_instance_id": wf_id,
            "name": event_name,
            "context": context,
        },
    }

    post_body = json.dumps(post_body)

    send_request("post", "/v1/workflow-instance-events:asyncCreate", data=post_body)

    return state_id


def start_workflow(wf_name, wf_version=None, context="", force_id=False):

    if (
        not hasattr(endtoend.testhandle, "workflow_definition_id_mapping")
        and not force_id
    ):
        raise WorkflowError(
            f"{datetime.utcnow()} - Workflow started but no workflows specified in test file"
        )
    request_id = uuid.uuid4().hex

    if force_id:
        workflow_definition_id = wf_name
    else:
        workflow_definition_id = endtoend.testhandle.workflow_definition_id_mapping[
            wf_name
        ]

    post_body = {
        "request_id": request_id,
        "workflow_instance": {
            "workflow_definition_id": workflow_definition_id,
            "workflow_definition_version_id": wf_version if wf_version else "",
        },
        "instantiation_context": context,
    }

    post_body = json.dumps(post_body)

    data = send_request("post", "/v1/workflow-instances", data=post_body)

    return data["id"]


def upload_workflow(workflow_definition_id, workflow_specification):
    request_id = uuid.uuid4().hex

    post_body = {
        "request_id": request_id,
        "workflow_definition_version": {
            "workflow_definition_id": workflow_definition_id,
            "specification": workflow_specification,
        },
    }

    post_body = json.dumps(post_body)
    resp = send_request("post", "/v1/workflow-definition-versions", data=post_body)

    return resp


def delete_workflow(workflow_definition_id):
    send_request(
        "delete", "/v1/workflow-definition-versions" + "/" + workflow_definition_id
    )
    log.info("Workflow %s deleted", workflow_definition_id)


def delete_all_workflows():
    for wfd_id in endtoend.testhandle.uploaded_workflows.values():
        delete_workflow(wfd_id)

    return


def check_workflow_version(wf_definition_id: str, wf_abs_path: str):
    """
    Check the Workflow version against the instance used for testing.

    An exception is raised if it on the instance at the same version but different content.

    :param wf_definition_id: The identifier of the workflow definition
    :param wf_abs_path: The absolute pathname for the workflow definition
    """
    # Open the workflow content and get the file version number
    with open(wf_abs_path, "r", encoding="utf-8") as wf_file:
        wf_yaml = wf_file.read()
    wf_parsed = yaml.safe_load(wf_yaml)
    file_version = wf_parsed["definition_version"]

    # If this workflow is loaded on the instance and at the same version,
    # check the content is the same
    wf_definition_version = workflows_api.get_workflow_definition_version(
        file_version + "," + wf_definition_id
    )
    if not wf_definition_version:
        log.warning(
            f"{wf_definition_id} {file_version} not present on env or network error"
        )
    else:
        from_instance = wf_definition_version["specification"]
        if from_instance != wf_yaml:
            raise WorkflowError(
                f"{datetime.utcnow()} -"
                f" Instance has different content for {wf_definition_id} {file_version}:"
                f" Version increment may be required"
            )
        else:
            log.info(f"{wf_definition_id} {file_version} matches on instance")


def create_workflow_definition_id_mapping():
    """
    Creates a mapping between original definition ids and run-specific ids, so
    that tests do not need to be aware of the modified ids
    :return:
    """

    if not hasattr(endtoend.testhandle, "WORKFLOWS"):
        return

    workflow_definition_id_mapping = {
        workflow_definition_id: generate_unique_workflow_definition_id(
            workflow_definition_id
        )
        for workflow_definition_id in endtoend.testhandle.WORKFLOWS
    }

    endtoend.testhandle.workflow_definition_id_mapping = workflow_definition_id_mapping


def update_and_upload_workflows():
    """
    Uploads the workflows required for a test suite using unique workflow definition ids per run to
    avoid conflicts.
    :return:
    """
    for (
        workflow_definition_id,
        workflow_file_path,
    ) in endtoend.testhandle.WORKFLOWS.items():
        if endtoend.testhandle.do_version_check:
            # TODO: we could cache the results as some workflows are used across multiple tests
            check_workflow_version(workflow_definition_id, workflow_file_path)
        update_and_upload_workflow(
            workflow_definition_id,
            workflow_file_path,
            endtoend.testhandle.workflow_definition_id_mapping,
            endtoend.testhandle.contract_pid_to_uploaded_pid,
            endtoend.testhandle.internal_account_id_to_uploaded_id,
            endtoend.testhandle.calendar_ids_to_e2e_ids,
            endtoend.testhandle.supervisorcontract_name_to_id,
        )


def update_and_upload_workflow(
    workflow_definition_id: str,
    workflow_file_path: str,
    workflow_definition_id_mapping: Dict[str, str],
    contract_pid_to_uploaded_pid: Dict[str, str],
    internal_account_id_to_uploaded_id: Dict[str, str],
    calendar_id_to_e2e_id: Dict[str, str],
    supervisorcontract_name_to_id: Dict[str, str],
) -> None:
    """
    Update workflow definition with:
     - new definition version, name and title to make it clear this is an e2e test copy
     - any mapped workflow ids and versions for child workflows
    Upload it to the e2e instance
    :param workflow_definition_id: id of the workflow definition to process
    :param workflow_file_path: path to the workflow definition
    :param workflow_definition_id_mapping: mapping of original workflow definition id to unique e2e
     id
    :param contract_pid_to_uploaded_pid: mapping of original product id to unique e2e id
    :param internal_account_id_to_uploaded_id: mapping of original internal account id to unique
     e2e id
    :param calendar_ids_to_e2e_ids: mapping of original calendar id to e2e id
    :param supervisorcontract_name_to_id: mapping of supervisor contract name to uploaded
     supervisor contract version id
    :return: None
    """

    mapped_workflow_definition_id = workflow_definition_id_mapping[
        workflow_definition_id
    ]

    if mapped_workflow_definition_id in endtoend.testhandle.uploaded_workflows:
        return

    with open(workflow_file_path, "r", encoding="utf-8") as workflow_file:
        data = workflow_file.read()

    parsed_definition = yaml.safe_load(data)

    parsed_definition["definition_version"] = "1.0.0"
    parsed_definition["name"] = "e2e " + parsed_definition["name"]
    if "instance_title" in parsed_definition:
        parsed_definition["instance_title"] = (
            "e2e " + parsed_definition["instance_title"]
        )

    # Any hardcoded ids must be replaced with the unique e2e ids
    parsed_definition = replace_child_workflow_definition_ids(
        parsed_definition, workflow_definition_id_mapping
    )

    parsed_definition = replace_product_ids(
        parsed_definition, contract_pid_to_uploaded_pid
    )

    parsed_definition = replace_internal_account_ids(
        parsed_definition, internal_account_id_to_uploaded_id
    )

    parsed_definition = replace_calendar_ids(
        parsed_definition,
        calendar_id_to_e2e_id,
    )

    parsed_definition = replace_supervisor_contract_version_ids(
        parsed_definition,
        supervisorcontract_name_to_id,
    )

    serialised_data = yaml.dump(parsed_definition)
    log.info("Uploading workflow %s", mapped_workflow_definition_id)
    workflow = upload_workflow(mapped_workflow_definition_id, serialised_data)

    endtoend.testhandle.uploaded_workflows[mapped_workflow_definition_id] = workflow[
        "id"
    ]


def replace_calendar_ids(
    parsed_definition: WorkflowDefinition,
    calendar_id_to_uploaded_id: Dict[str, str],
) -> WorkflowDefinition:
    if calendar_id_to_uploaded_id:
        calendar_expr: JSONPath = parse("$..calendar_ids")
        children = [match.value for match in calendar_expr.find(parsed_definition)]
        for i, child_list in enumerate(children):
            for j, child in enumerate(child_list):
                if child in calendar_id_to_uploaded_id:
                    children[i][j] = calendar_id_to_uploaded_id[child]
                else:
                    raise WorkflowError(
                        f"Could not update workflow with E2E calendar id. Calendar id {child} is "
                        f"missing from e2e calendar mapping. Check it is included in the relevant "
                        f"test's endtoend.testhandle.CALENDARS"
                    )

    return parsed_definition


def replace_child_workflow_definition_ids(
    parsed_definition: WorkflowDefinition,
    workflow_definition_id_mapping: Dict[str, str],
) -> WorkflowDefinition:
    children_wf_expr: JSONPath = parse("$..instantiate_workflow")
    children_wf = [match.value for match in children_wf_expr.find(parsed_definition)]
    for child_wf in children_wf:
        if child_wf["definition_id"] in workflow_definition_id_mapping:
            child_wf["definition_id"] = workflow_definition_id_mapping[
                child_wf["definition_id"]
            ]
            child_wf["definition_version"] = "1.0.0"
        else:
            raise WorkflowError(
                f"Could not update workflow with E2E workflow definition id. Child workflow "
                f"definition id {child_wf['definition_id']} is missing from e2e"
                f"workflow mapping. Check it is included in the relevant test's "
                f"endtoend.testhandle.WORKFLOWS"
            )

    return parsed_definition


def replace_internal_account_ids(
    parsed_definition: WorkflowDefinition,
    internal_account_id_to_uploaded_id: Dict[str, str],
):
    if internal_account_id_to_uploaded_id:
        # INC-3757 - there isn't a pattern currently in library that requires this expression to
        #            be used for internal account id replacement 'parse("$..internal_account.id")'
        internal_account_id_expr: JSONPath = parse("$..internal_account_id.`parent`")

        internal_accounts = [
            match.value for match in internal_account_id_expr.find(parsed_definition)
        ]

        for internal_account in internal_accounts:
            if (
                internal_account["internal_account_id"]
                in internal_account_id_to_uploaded_id
            ):
                internal_account[
                    "internal_account_id"
                ] = internal_account_id_to_uploaded_id[
                    internal_account["internal_account_id"]
                ]
            else:
                raise WorkflowError(
                    f"Could not update workflow with E2E internal account id. Internal account "
                    f"id {internal_account['internal_account_id']} is missing from e2e"
                    f"internal account mapping. Check it is included in the relevant test's "
                    f"endtoend.testhandle.TSIDE_TO_INTERNAL_ACCOUNT_ID"
                )

    return parsed_definition


def replace_product_ids(
    parsed_definition: WorkflowDefinition, contract_pid_to_uploaded_pid: Dict[str, str]
) -> WorkflowDefinition:
    # Modify workflow to use uploaded contract
    if contract_pid_to_uploaded_pid:
        product_id_expr: JSONPath = parse("$..product_id.`parent`")
        contracts = [match.value for match in product_id_expr.find(parsed_definition)]
        for contract in contracts:
            if contract["product_id"] in contract_pid_to_uploaded_pid:
                contract["product_id"] = contract_pid_to_uploaded_pid[
                    contract["product_id"]
                ]
            # the product id can be a reference to a context variable which we don't want to map
            elif not contract["product_id"].startswith("${"):
                raise WorkflowError(
                    f"Could not update workflow with E2E product id. Product "
                    f"id {contract['product_id']} is missing from e2e "
                    f"product mapping. Check it is included in the relevant test's "
                    f"endtoend.testhandle.CONTRACTS"
                )

    return parsed_definition


def replace_supervisor_contract_version_ids(
    parsed_definition: WorkflowDefinition,
    supervisorcontract_name_to_id: Dict[str, str],
) -> WorkflowDefinition:

    scvi_expr: JSONPath = parse("$..supervisor_contract_version_id.`parent`")
    scvi_parents = [match.value for match in scvi_expr.find(parsed_definition)]
    for scvi_parent in scvi_parents:
        # we use _contract_version suffix for CLU but not in e2e test requirements
        scvi = re.sub(
            pattern=r"&\{(\w+)_supervisor_contract_version\}",
            repl=r"\1",
            string=scvi_parent["supervisor_contract_version_id"],
        )
        if scvi in supervisorcontract_name_to_id:
            scvi_parent[
                "supervisor_contract_version_id"
            ] = supervisorcontract_name_to_id[scvi]
        else:
            raise WorkflowError(
                f"Could not update workflow with E2E supervisor contract version id. SCVI"
                f" {scvi} is missing from e2e supervisor contract id mapping. Check it is "
                f" included in the relevant test's endtoend.testhandle.SUPERVISORCONTRACTS"
            )

    return parsed_definition


def generate_unique_workflow_definition_id(wf_definition_id: str) -> str:
    """
    Generates a pseudo-unique workflow definition id given an original workflow definition id
    :param wf_definition_id: the original definition id
    :return: the unique workflow definition id
    """
    random_chars = "".join(random.choice(string.ascii_letters) for x in range(10))

    return wf_definition_id + "_e2e_" + random_chars.upper()


@kafka_only_helper
def wait_for_wf_instantiation_messages(
    account_ids: List[str],
    expected_wf: Dict[str, int],
    matched_message_timeout: int = 30,
    inter_message_timeout: int = 30,
) -> Dict[str, Dict[str, Dict[str, int]]]:
    """
    Listen to messages on the create workflow request topic for matching workflows
    (account_id and wf_definition_id). Returns the difference betweeen expected and
    seen workflows for each account. Note that the account_id must exist in the
    instantiation context of the workflow.
    :param account_ids: List of account ids
    :param expected_wf: Dictionary of wf_definition_id: number of expected instantiations
    :return: Dictionary of acc_id for which the seen workflows do not match expected workflows
    """
    consumer = endtoend.testhandle.kafka_consumers[CREATE_WORKFLOW_INSTANCE_TOPIC]
    unique_message_ids = _generate_expected_workflows_object_dict(
        account_ids, expected_wf
    )

    def matcher(event_msg, unique_message_ids):
        event_request_id = event_msg["request_id"]
        event_wf_definition_id = event_msg["workflow_instance"][
            "workflow_definition_id"
        ]
        event_account_id = event_msg["instantiation_context"].get("account_id")
        is_matched = False
        if event_account_id in unique_message_ids:
            wf_id_missing = True
            for expected_workflow in unique_message_ids[event_account_id]:
                if expected_workflow.workflow_definition_id == event_wf_definition_id:
                    wf_id_missing = False
                    if event_request_id not in expected_workflow.request_ids:
                        expected_workflow.count -= 1
                        expected_workflow.request_ids.add(event_request_id)
                        is_matched = True
                        break
            if wf_id_missing:
                # then a wf has been instantiated for this acc_id and
                # is not listed in the profile.yaml
                unique_message_ids[event_account_id].append(
                    ExpectedWorklowInstantiation(
                        workflow_definition_id=event_wf_definition_id,
                        count=-1,
                        request_ids={event_request_id},
                    )
                )

        return None, event_request_id, is_matched

    seen_workflows = wait_for_messages(
        consumer=consumer,
        matcher=matcher,
        callback=None,
        unique_message_ids=unique_message_ids,
        matched_message_timeout=matched_message_timeout,
        inter_message_timeout=inter_message_timeout,
    )

    wfs_missing = {}
    for acc_id, wf_details in seen_workflows.items():
        for expected_wf_instantiation in wf_details:
            if expected_wf_instantiation.count != 0:
                if acc_id not in wfs_missing:
                    wfs_missing[acc_id] = {}

                wfs_missing[acc_id][
                    expected_wf_instantiation.workflow_definition_id
                ] = {
                    "expected": expected_wf.get(
                        expected_wf_instantiation.workflow_definition_id, 0
                    ),
                    "seen": expected_wf.get(
                        expected_wf_instantiation.workflow_definition_id, 0
                    )
                    - expected_wf_instantiation.count,
                }

    return wfs_missing


def _generate_expected_workflows_object_dict(
    account_ids: List[str], expected_wf: Dict[str, int]
) -> Dict[str, List[ExpectedWorklowInstantiation]]:
    """
    Generates a dictionary of ExpectedWorklowInstantiation objects for each account id
    :param account_ids: List of account ids
    :param expected_wf: Dictionary of wf_definition_id: number of expected instantiations
    :return: unique Dict of account_id: [ExpectedWorklowInstantiation]
    """

    return {
        acc_id: [
            ExpectedWorklowInstantiation(workflow_definition_id=wf_id, count=count)
            for wf_id, count in expected_wf.items()
        ]
        for acc_id in account_ids
    }


if __name__ == "__main__":
    pass
