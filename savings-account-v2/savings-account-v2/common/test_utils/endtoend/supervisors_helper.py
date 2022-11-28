# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.

# standard libs
import hashlib
import json
import logging
import os
import random
import re
import string
import unittest
import uuid
from typing import Any, Dict, List, Union

# common
import common.test_utils.endtoend as endtoend
from common.test_utils.common.utils import replace_supervisee_version_ids_in_supervisor
from common.test_utils.endtoend.kafka_helper import kafka_only_helper, wait_for_messages

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


PLAN_UPDATE_EVENTS_TOPIC = "vault.core_api.v1.plans.plan_update.events"

TERMINAL_PLAN_STATUSES = [
    "PLAN_UPDATE_STATUS_REJECTED",
    "PLAN_UPDATE_STATUS_ERRORED",
    "PLAN_UPDATE_STATUS_COMPLETED",
]


def create_supervisor_contract(display_name, supervisor_contract_id=""):
    display_name = display_name
    randomchars = "".join(random.choice(string.ascii_letters) for x in range(10))
    supervisor_contract_id = supervisor_contract_id + randomchars
    request_hash = str(
        hashlib.md5(
            (display_name + supervisor_contract_id or "").encode("utf-8")
        ).hexdigest()
    )

    post_body = {
        "supervisor_contract": {
            "id": supervisor_contract_id,
            "display_name": display_name,
        },
        "request_id": request_hash,
    }

    post_body = json.dumps(post_body)

    resp = endtoend.helper.send_request(
        "post", "/v1/supervisor-contracts", data=post_body
    )
    log.info("Supervisor contract %s created.", resp["id"])

    return resp


def create_supervisor_contract_version(
    supervisor_contract_id,
    display_name,
    code,
    description=None,
    supervisor_contract_version_id=None,
):

    e2e_display_name = "e2e_" + display_name

    if not description:
        description = "Description of " + e2e_display_name

    code_hash = hashlib.md5((code).encode("utf-8")).hexdigest()

    post_body = {
        "supervisor_contract_version": {
            "id": supervisor_contract_version_id,
            "supervisor_contract_id": supervisor_contract_id,
            "display_name": e2e_display_name,
            "description": description,
            "code": code,
        },
        "request_id": code_hash,
    }

    post_body = json.dumps(post_body)

    resp = endtoend.helper.send_request(
        "post", "/v1/supervisor-contract-versions", data=post_body
    )
    log.info("Supervisor contract %s version uploaded.", resp["id"])
    endtoend.testhandle.supervisorcontract_name_to_id[display_name] = resp["id"]

    return resp


def upload_supervisor_contracts(supervisor_contracts):
    # The supervisee alias may be different from the contract ID used in the E2E test file.
    # We need this mapping to easily replace the specified alias with the uploaded contracts
    supervisee_alias_to_version_id = {
        endtoend.testhandle.CONTRACTS[pid].get(
            "supervisee_alias", pid
        ): endtoend.contracts_helper.get_current_product_version_id(pid)
        for pid in endtoend.testhandle.contract_pid_to_uploaded_pid
    }

    for product_id, contract_properties in supervisor_contracts.items():
        if "path" not in contract_properties:
            raise NameError(
                "Contract: {} not specified with path. "
                "Specified with {}".format(product_id, str(contract_properties))
            )

        contractfile = contract_properties["path"]
        with open(contractfile, "r") as cfile:
            contractdata = cfile.read()

        supervisor_contract = create_supervisor_contract(product_id)

        contractdata = replace_supervisee_version_ids_in_supervisor(
            contractdata, supervisee_alias_to_version_id
        )

        create_supervisor_contract_version(
            supervisor_contract["id"], product_id, contractdata
        )


def create_plan(
    supervisor_contract_version_id: str, account_id: str = None, details: Dict = None
) -> str:
    request_id = uuid.uuid4().hex

    post_body = {
        "plan": {
            "id": account_id,
            "supervisor_contract_version_id": supervisor_contract_version_id,
            "details": details,
        },
        "request_id": request_id,
    }

    post_body = json.dumps(post_body)

    resp = endtoend.helper.send_request("post", "/v1/plans", data=post_body)
    log.info("Plan %s created.", resp["id"])
    endtoend.testhandle.plans.append(resp["id"])

    return resp["id"]


def close_all_plans():
    for plan_id in endtoend.testhandle.plans:
        close_plan(plan_id)


def close_plan(plan_id):
    request_id = uuid.uuid4().hex

    post_body = {
        "plan_update": {"plan_id": plan_id, "closure_update": {}},
        "request_id": request_id,
    }

    post_body = json.dumps(post_body)

    resp = endtoend.helper.send_request("post", "/v1/plan-updates", data=post_body)
    log.info("Close plan: %s for plan: %s sent.", resp["id"], plan_id)

    return resp


def get_plan_update(plan_update_id: str) -> Dict:
    params = {"ids": [plan_update_id]}
    resp = endtoend.helper.send_request(
        "get", "/v1/plan-updates:batchGet", params=params
    )

    return next(iter(resp["plan_updates"].values()))


def get_plan_updates_by_ids(plan_update_ids: List[str]) -> Dict[str, Dict]:
    """
    Fetch details for one or more plan update ids.
    :param plan_update_ids: a collection of plan update ids
    :return: dict with id and update plan information
    """
    params = {"ids": plan_update_ids}
    resp = endtoend.helper.send_request(
        "get", "/v1/plan-updates:batchGet", params=params
    )
    return resp["plan_updates"]


def create_plan_update(
    plan_id: str,
    plan_action_type: str,
    action: Dict[str, Any],
    status: str = None,
    account_id: str = None,
) -> Dict[str, Any]:
    request_id = uuid.uuid4().hex

    post_body = {
        "plan_update": {"id": account_id, "plan_id": plan_id, "status": status},
        "request_id": request_id,
    }

    post_body["plan_update"][plan_action_type] = action

    post_body = json.dumps(post_body)

    resp = endtoend.helper.send_request("post", "/v1/plan-updates", data=post_body)
    log.info("Plan_update: %s for plan: %s sent.", resp["id"], plan_id)

    return resp


def wait_for_plan_updates(
    plan_update_ids: List[str], target_status="PLAN_UPDATE_STATUS_COMPLETED"
) -> None:

    """
    Verrify if given one or more plan update ids are of target status.
    :param plan_update_ids: a collection of plan update ids
    :param target_status: the plan update status to wait for
    """
    if endtoend.testhandle.use_kafka:
        wait_for_plan_updates_by_id(
            plan_update_ids=plan_update_ids,
            target_status=target_status,
        )
    else:
        # result_wrapper verifies if all plan_updates have target_status
        # and get_plan_updates_by_ids was able to fetch details for all requested ids
        endtoend.helper.retry_call(
            func=get_plan_updates_by_ids,
            f_args=[plan_update_ids],
            expected_result=True,
            result_wrapper=lambda data: all(
                item["status"] == target_status for _, item in data.items()
            )
            and data.keys() == set(plan_update_ids),
            failure_message=f'"One of plan updates in {plan_update_ids} never completed.\n"',
        )


@kafka_only_helper
def wait_for_plan_updates_by_id(
    plan_update_ids: List[str],
    target_status: str = "PLAN_UPDATE_STATUS_COMPLETED",
) -> None:
    """
    Listen to the plan update events Kafka topic for specific plan update ids.
    :param plan_update_ids: a collection of plan update ids to listen for
    :param target_status: the plan update status to wait for
    """
    consumer = endtoend.testhandle.kafka_consumers[PLAN_UPDATE_EVENTS_TOPIC]

    def matcher(event_msg, unique_message_ids):
        if target_status == "PLAN_UPDATE_STATUS_PENDING_EXECUTION":
            plan_update_wrapper = event_msg.get("plan_update_created")
        else:
            plan_update_wrapper = event_msg.get("plan_update_updated")
        event_request_id = event_msg["event_id"]
        if plan_update_wrapper:
            plan_update = plan_update_wrapper["plan_update"]
            if plan_update["id"] in unique_message_ids:
                if plan_update["status"] == target_status:
                    return plan_update["id"], event_request_id, True

                if plan_update["status"] in TERMINAL_PLAN_STATUSES:
                    log.warning(
                        f"Plan update {plan_update['id']} returned a status of "
                        f"{plan_update['status']}"
                    )
        return "", event_request_id, False

    failed_plan_updates = wait_for_messages(
        consumer,
        matcher=matcher,
        callback=None,
        unique_message_ids={update_id: None for update_id in plan_update_ids},
        inter_message_timeout=30,
        matched_message_timeout=30,
    )

    if len(failed_plan_updates) > 0:
        raise Exception(
            f"Failed to retrieve {len(failed_plan_updates)} of {len(plan_update_ids)} "
            f"plan updates for update ids: {', '.join(failed_plan_updates)}"
        )


def create_and_wait_for_plan_update(
    plan_id: str,
    plan_action_type: str,
    action: Dict[str, Any],
    status: str = None,
    account_id: str = None,
) -> Dict[str, Any]:
    plan_update = create_plan_update(
        plan_id, plan_action_type, action, status, account_id
    )
    plan_update_id = plan_update["id"]
    wait_for_plan_updates([plan_update_id])
    return plan_update


def add_account_to_plan(plan_id, account_id):
    action = {"account_id": account_id}
    log.info(f"preparing to link account {account_id} to plan {plan_id}")
    return create_and_wait_for_plan_update(plan_id, "associate_account_update", action)


def link_accounts_to_supervisor(supervisor_contract, account_list):
    supervisor_contract_version_id = endtoend.testhandle.supervisorcontract_name_to_id[
        supervisor_contract
    ]
    plan_id = create_plan(supervisor_contract_version_id)

    for account in account_list:
        add_account_to_plan(plan_id, account)

    return plan_id


def get_plan_associations(account_ids=None, plan_ids=None):
    if not account_ids and not plan_ids:
        raise NameError("account id nor plan id specified")
    if account_ids and not isinstance(account_ids, list):
        account_ids = [account_ids]
    if plan_ids and not isinstance(plan_ids, list):
        plan_ids = [plan_ids]

    params = {"account_ids": account_ids, "plan_ids": plan_ids, "page_size": "100"}

    resp = endtoend.helper.send_request("get", "/v1/account-plan-assocs", params=params)

    return resp["account_plan_assocs"]


def get_plan_schedules(plan_id=None, page_size="20"):
    if not plan_id:
        raise NameError("plan id not specified")

    params = {"plan_id": plan_id, "page_size": page_size}

    resp = endtoend.helper.send_request("get", "/v1/plan-schedules", params=params)

    schedule_ids = [s["id"] for s in resp["plan_schedules"]]

    response_schedules = endtoend.core_api_helper.get_schedules(schedule_ids)

    # A dict of schedule event_names to their schedule objects
    return {
        schedule_details["display_name"].split()[0]: schedule_details
        for schedule_details in response_schedules.values()
        if schedule_details["status"] != "SCHEDULE_STATUS_DISABLED"
        and re.search(rf"{plan_id}", schedule_details["display_name"])
    }


def get_plan_details(plan_id):

    params = {"ids": plan_id}

    resp = endtoend.helper.send_request("get", "/v1/plans:batchGet", params=params)

    return resp["plans"][plan_id]


def check_plan_associations(
    test: unittest.TestCase, plan_id: str, accounts: Union[List[str], Dict[str, str]]
):
    """
    Helper method to validate that plan currently has expected associations. If a given account has
    been through multiple associations with the same plan, only the latest is considered
    :param plan_id: the plan id
    :param account_ids: the account ids to validate are currently linked. If passed as a list, the
    link statuses are assumed to be active. If passed as a dict, the values are the statuses and the
    keys are the account ids.
    """
    plan_associations = endtoend.supervisors_helper.get_plan_associations(
        plan_ids=plan_id
    )

    # there could be multiple assocs with different statuses, but we'll only consider the latest
    actual_linked_accounts = {
        association["account_id"]: association["status"]
        for association in plan_associations
    }

    if isinstance(accounts, list):
        accounts = {
            account_id: "ACCOUNT_PLAN_ASSOC_STATUS_ACTIVE" for account_id in accounts
        }

    test.assertEqual(
        actual_linked_accounts,
        accounts,
        "latest and expected associations do not match",
    )
