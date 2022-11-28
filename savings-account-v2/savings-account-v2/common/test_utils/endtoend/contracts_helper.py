# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# standard libs
import collections
import hashlib
import json
import logging
import os
import re
import time
import uuid
import random
import string
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Union

# third party
import yaml
from dateutil.relativedelta import relativedelta
from requests import HTTPError

# common
import common.test_utils.endtoend as endtoend
from common.python.file_utils import load_file_contents
from common.test_utils.endtoend.core_api_helper import AccountStatus
from common.test_utils.endtoend.helper import SetupError

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

VALID_ACCOUNT_SCHEDULE_TAG_TIME_DELTA_FIELDS = (
    "test_pause_at_timestamp",
    "schedule_status_override_start_timestamp",
    "schedule_status_override_end_timestamp",
)

DUMMY_CONTRA = "DUMMY_CONTRA"


def upload_contracts(contracts: Dict[str, Dict[str, Any]]) -> None:
    """
    Uploads contracts and creates a mapping between original product ids and run-specific ids, so
    that tests do not need to be aware of the modified ids
    :param contracts: Dict[str, Dict[str, object]], map of product ids and the corresponding
    dictionary of contract properties
    :return:
    """
    for product_id, contract_properties in contracts.items():
        if "path" not in contract_properties:
            raise NameError(
                "Contract: {} not specified with path. "
                "Specified with {}".format(product_id, str(contract_properties))
            )

        contractfile = contract_properties["path"]
        contractdata = load_file_contents(contractfile)

        if endtoend.testhandle.do_version_check:
            check_product_version(product_id, contractdata)

        contractdata = replace_workflow_ids_with_e2e_ids(contractdata)

        contractdata = replace_schedule_tag_ids_with_e2e_ids(contractdata)

        contractdata = replace_calendar_ids_with_e2e_ids(contractdata)

        parameters = contract_properties.get("template_params", {})
        supported_denominations = contract_properties.get("supported_denoms", ["GBP"])
        is_internal = contract_properties.get("is_internal", False)
        display_name = contract_properties.get("display_name", "")

        for param_name, param_value in parameters.items():
            if "_account" in param_name.lower() and type(param_value) is not dict:
                log.warning(
                    f"If {param_name}, is an internal account that should be converted, ensure "
                    f"that the parameter uses a dictionary with key internal_account_key, e.g. "
                    f""
                    '"accrued_interest_payable_account"'
                    ": {"
                    '"internal_account_key"'
                    ": ACCRUED_INTEREST_PAYABLE_ACCOUNT}"
                )

            if type(param_value) is dict:
                if param_value.get("internal_account_key"):
                    parameters[
                        param_name
                    ] = endtoend.testhandle.internal_account_id_to_uploaded_id[
                        param_value["internal_account_key"]
                    ]
        new_params = [
            {"name": param_name, "value": param_value}
            for param_name, param_value in parameters.items()
        ]
        ordered_params = str(collections.OrderedDict(sorted(parameters.items())))

        code_hash = hashlib.md5(
            (
                contractdata
                + str(supported_denominations)
                + display_name
                + ordered_params
                or ""
            ).encode("utf-8")
        ).hexdigest()
        e2e_unique_product_id = "e2e_" + product_id + "_" + code_hash

        resp = endtoend.core_api_helper.create_product_version(
            request_id=e2e_unique_product_id,
            code=contractdata,
            product_id=e2e_unique_product_id,
            supported_denominations=supported_denominations,
            tags=[],
            params=new_params,
            is_internal=is_internal,
            migration_strategy="PRODUCT_VERSION_MIGRATION_STRATEGY_UNKNOWN",
            contract_properties=contract_properties,
        )

        # Vault may have already seen this code with a different product id
        log.info("Contract %s uploaded.", resp["product_id"])
        if is_internal:
            endtoend.testhandle.internal_contract_pid_to_uploaded_pid[
                product_id
            ] = e2e_unique_product_id
        else:
            endtoend.testhandle.contract_pid_to_uploaded_pid[
                product_id
            ] = e2e_unique_product_id


def create_account(
    customer: Union[str, list],
    contract: str,
    instance_param_vals: Optional[Dict[str, str]] = None,
    permitted_denominations: Optional[List[str]] = None,
    status: str = "ACCOUNT_STATUS_OPEN",
    details: Optional[Dict[str, str]] = None,
    force_contract_id: bool = False,
    wait_for_activation: bool = True,
    opening_timestamp: str = None,
) -> Dict[str, Any]:
    """
    :param customer: the customer id to create the account for
    :param contract: the product id to instantiate the account with
    :param instance_param_vals:
    :param permitted_denominations: Defaults to GBP
    :param status: the account status to create the account with. One of ACCOUNT_STATUS_PENDING or
     ACCOUNT_STATUS_OPEN
    :param force_contract_id: if True the actual product id is used instead of the e2e product id
    :param details: account details metadata to add to the account
    :param wait_for_activation: if True the account will only be returned once the activation
    account-update is completed
    have been initialised
    :param opening_timestamp: the time when the account was opened. If supplied during account
    creation, the account must be created with status ACCOUNT_STATUS_OPEN and the opening_timestamp
    value must not be a time in the future.
    :return: the account resource
    """
    request_id = uuid.uuid4().hex
    permitted_denominations = permitted_denominations or ["GBP"]
    if (
        contract not in endtoend.testhandle.contract_pid_to_uploaded_pid
        and not force_contract_id
    ):
        raise NameError(
            "Contract ID: {} not found. "
            "Is it specified in the testfile?".format(contract)
        )

    post_body = {
        "request_id": request_id,
        "account": {
            "product_id": contract
            if force_contract_id
            else (endtoend.testhandle.contract_pid_to_uploaded_pid[contract]),
            "stakeholder_ids": [customer] if type(customer) is not list else customer,
            "instance_param_vals": instance_param_vals,
            "permitted_denominations": permitted_denominations,
            "status": status,
            "details": details,
            "opening_timestamp": opening_timestamp,
        },
    }

    post_body = json.dumps(post_body)

    account = endtoend.helper.send_request("post", "/v1/accounts", data=post_body)
    account_id = account["id"]
    endtoend.testhandle.accounts.add(account_id)
    log.info(
        f"Created account {account_id} for customer {customer} with product {contract}"
    )
    if wait_for_activation:
        endtoend.accounts_helper.wait_for_account_update(
            account_id, "activation_update"
        )

    return account


def get_account(account_id):
    resp = endtoend.helper.send_request("get", "/v1/accounts/" + account_id)
    return resp


def create_internal_account(
    account_id: str,
    contract: str,
    accounting_tside: str,
    details: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    :param id: the internal account id used to create a specific account
    :param contract: the product id to instantiate the account with
    :param accounting_tside: specifies the account tside
    :param permitted_denominations: Defaults to GBP
    :param details: account details metadata to add to the account
    :return: the internal account resource
    """
    request_id = uuid.uuid4().hex
    tside = "A" if accounting_tside == "TSIDE_ASSET" else "L"

    e2e_composite_id = "e2e_" + tside + "_" + account_id
    if len(e2e_composite_id) > 32:
        raise SetupError(
            f"Internal account id {e2e_composite_id} is longer than 32 characters."
            " Please use an internal account id that is up to 26 characters long"
        )

    if contract not in endtoend.testhandle.internal_contract_pid_to_uploaded_pid:
        raise NameError(
            "Contract ID: {} not found. "
            "Is it specified in the testfile?".format(contract)
        )

    post_body = {
        "request_id": request_id,
        "internal_account": {
            "id": e2e_composite_id,
            "product_id": endtoend.testhandle.internal_contract_pid_to_uploaded_pid[
                contract
            ],
            "details": details,
            "accounting": {"tside": accounting_tside},
        },
    }
    post_body = json.dumps(post_body)
    internal_account = endtoend.helper.send_request(
        "post", "/v1/internal-accounts", data=post_body
    )

    endtoend.testhandle.internal_account_id_to_uploaded_id[
        account_id
    ] = internal_account["id"]

    return internal_account


def get_internal_account(account_id):
    resp = endtoend.helper.send_request("get", "/v1/internal-accounts/" + account_id)
    return resp


def get_all_balances(account_id):
    body = {"account_id": account_id, "live": True, "page_size": "100"}
    resp = endtoend.helper.send_request("get", "/v1/balances", params=body)

    return resp


def get_specific_balance(
    account_id,
    address,
    asset="COMMERCIAL_BANK_MONEY",
    phase="POSTING_PHASE_COMMITTED",
    denomination="GBP",
    sleep=0,
):
    time.sleep(sleep)
    balances = get_all_balances(account_id)

    for balance in balances["balances"]:
        if (
            balance["account_address"] == address
            and balance["asset"] == asset
            and balance["phase"] == phase
            and balance["denomination"] == denomination
        ):
            return balance["amount"]

    return "0"


def some_non_zero_balances_exist(account_id: str) -> bool:
    """
    Checks if any non-zero balances exist for a given account
    :param account_id: if of the account to check balances for
    :return: True if any non-zero balances exist, False otherwise
    """

    balances = get_all_balances(account_id)

    for balance in balances["balances"]:
        if balance["amount"] != "0":
            return True

    return False


def clear_balances(account_handle):
    account_id = account_handle["id"]

    balances = get_all_balances(account_id)

    if account_handle["accounting"]["tside"] == "TSIDE_LIABILITY":
        liability_account = True
    else:
        liability_account = False

    for balance in balances["balances"]:
        if balance["amount"] != "0":
            amount = Decimal(balance["amount"])
            postings = []
            credit = (amount < 0 and liability_account) or (
                amount > 0 and not liability_account
            )
            postings.append(
                endtoend.postings_helper.create_posting(
                    account_id=account_id,
                    amount=str(abs(amount)),
                    denomination=balance["denomination"],
                    asset=balance["asset"],
                    account_address=balance["account_address"],
                    phase=balance["phase"],
                    credit=credit,
                )
            )
            postings.append(
                endtoend.postings_helper.create_posting(
                    account_id=endtoend.testhandle.internal_account,
                    amount=str(abs(amount)),
                    denomination=balance["denomination"],
                    asset=balance["asset"],
                    account_address="DEFAULT",
                    phase=balance["phase"],
                    credit=not credit,
                )
            )
            # withdrawal_override & calendar_override needed to force the funds out of TD
            # todo: make this use output from KERN-I-26

            pib_id = endtoend.postings_helper.create_custom_instruction(
                postings,
                batch_details={
                    "withdrawal_override": "true",
                    "calendar_override": "true",
                },
            )
            # ensure that the balances have been updated for this pib
            endtoend.balances_helper.wait_for_posting_balance_updates(
                account_id=account_id,
                posting_instruction_batch_id=pib_id,
            )

    # TODO: Add back in after TM-24384 is resolved to fix wallet e2e
    # endtoend.helper.retry_call(
    #     func=some_non_zero_balances_exist,
    #     f_args=[account_handle],
    #     expected_result=False
    # )
    return


def clear_account_balances(account):

    account_id = account["id"]

    # This needs revisiting. The 3 attempts are for 1) initial clean-up, 2) clean-up of any
    # unintended side-effects, 3) final attempt before giving up. It will never work for some
    # contracts. It is also prone to flakes as the balances may not have updated between attempts
    for _ in range(3):
        if some_non_zero_balances_exist(account_id):
            clear_balances(account)

    if some_non_zero_balances_exist(account_id):
        raise Exception(
            f"{datetime.utcnow()} - "
            f"Balances aren't being cleared for account {account_id}.",
            f"Latest balances:\n{get_all_balances(account_id)}",
        )


def clear_specific_address_balance(
    account_handle: Dict[str, Any], address: str, denomination: str = "GBP"
) -> bool:
    """
    Asserts balance of provided address, sends posting DEFAULT to negate that amount
    :param account_handle: the account resource
    :param address: the address to be cleared.
    :param denomination: denomination of the address to be cleared.
    :return: True if a posting was sent to clear the balance else return false.
    """

    # TODO: Would it be worth expanding to allow custom instructions to zero specific addresses
    #  instead of DEFAULT?
    account_id = account_handle["id"]

    balance = get_specific_balance(account_id, address, denomination=denomination)

    if balance == "0":
        log.info("Balance returned zero, retrying to confirm")
        time.sleep(5)
        balance = get_specific_balance(account_id, address, denomination=denomination)

    if account_handle["accounting"]["tside"] == "TSIDE_LIABILITY":
        liability_account = True
    else:
        liability_account = False

    log.info(f"Clear initiated for {address}")

    if balance != "0":
        amount = Decimal(balance)
        credit = (amount < 0 and liability_account) or (
            amount > 0 and not liability_account
        )

        if credit:
            endtoend.postings_helper.inbound_hard_settlement(
                account_id=account_id,
                amount=str(abs(amount)),
                denomination=denomination,
                override=True,
                batch_details={"withdrawal_override": "true"},
            )
        else:
            endtoend.postings_helper.outbound_hard_settlement(
                account_id=account_id,
                amount=str(abs(amount)),
                denomination=denomination,
                override=True,
                batch_details={"withdrawal_override": "true"},
            )
        return True

    return False


def terminate_account(account: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an account to the appropriate terminal status. For accounts that are still in PENDING
    status this is the CANCELLED status. For accounts that are in OPEN or PENDING_CLOSURE status
    this is the CLOSED status
    :param account: the account to terminate
    :return: the terminated account
    """
    account_id = account["id"]
    endtoend.accounts_helper.wait_for_all_account_updates_to_complete(account_id)

    account_status = AccountStatus(account["status"])

    if account_status in [
        AccountStatus.ACCOUNT_STATUS_CLOSED,
        AccountStatus.ACCOUNT_STATUS_CANCELLED,
    ]:
        return account

    elif account_status is AccountStatus.ACCOUNT_STATUS_PENDING:
        # Unactivated accounts should be cancelled instead of closed
        return endtoend.core_api_helper.update_account(
            account_id, AccountStatus.ACCOUNT_STATUS_CANCELLED
        )

    elif account_status is not AccountStatus.ACCOUNT_STATUS_PENDING_CLOSURE:
        account = endtoend.core_api_helper.update_account(
            account["id"], AccountStatus.ACCOUNT_STATUS_PENDING_CLOSURE
        )
        endtoend.accounts_helper.wait_for_account_update(account_id, "closure_update")

    clear_account_balances(account)

    return endtoend.core_api_helper.update_account(
        account_id, AccountStatus.ACCOUNT_STATUS_CLOSED
    )


def teardown_all_accounts():

    fail_count = 0
    for account_id in endtoend.testhandle.accounts:
        try:
            account = get_account(account_id)
            terminate_account(account)
        # We want to continue tearing down all accounts even if one fails
        except BaseException as e:
            fail_count += 1
            log.exception(f"Failed to teardown account {account_id}: {e.args}")

    # Raise a single exception if one or more teardowns failed as this warrants investigation
    if fail_count > 0:
        raise Exception(
            f"{datetime.utcnow()} - Failed to teardown {fail_count} accounts"
        )


def update_product_parameter(
    product_id: str, params_to_update: Dict[str, str]
) -> Dict[str, str]:
    product_version_id = get_current_product_version_id(product_id)

    items_to_add = [
        {"name": param_name, "value": param_value}
        for param_name, param_value in params_to_update.items()
    ]
    data = {"request_id": str(uuid.uuid4()), "items_to_add": items_to_add}

    resp = endtoend.helper.send_request(
        "put",
        f"/v1/product-versions/{product_version_id}:updateParams",
        data=json.dumps(data),
    )

    return resp


def get_current_product_version_id(product_id: str, e2e: bool = True) -> str:
    """
    Returns version id of the product from the specified instance.
    One can specify whether the version id of the e2e product or the original product
    corresponding to the product_id is returned - done through the e2e parameter.

    :param product_id: str Smart contract id as specifed in the file
    :param e2e: bool
    """
    unique_product_id = product_id
    if e2e:
        unique_product_id = endtoend.testhandle.contract_pid_to_uploaded_pid[product_id]

    params = {"ids": unique_product_id}

    resp = endtoend.helper.send_request("get", "/v1/products:batchGet", params=params)

    return resp["products"][unique_product_id]["current_version_id"]


def _replace_resource_ids_with_e2e_ids(
    contractdata: str,
    resource_type: str,
    id_regex: str,
    id_to_e2e_id_mapping: Dict[str, str],
) -> str:
    r"""
    Generic method to replace resource ids in contracts (e.g. workflow definitions, scheduler tags)
    with the test-run-specific e2e ids.
    :param contractdata: the contract to replace the resource ids in
    :param resource_type: the type of resources to replace. Only used for logging purposes
    :param id_regex: the regular expression to identify resource ids to replace. The regex makes use
    of following named groups:
        - 'prefix' - Optional - anything preceding the identifiers that is needed to identify them
        - 'ids' - the identifiers themselves. These can be comma separated and surrounded by single
        quotes or double quotes (these are replaced by single quotes). Whitespace between
        identifiers is also supported
        - 'suffix' - Optional - anything following the identifiers that is needed to identify them
    The regex match is then replaced by combining the prefix, the mapped identifiers found inside
    the main group, and the suffix
    For example, a contract containing scheduler_tags_ids=['id_1', "id_2"] would use regex
    '(?P<prefix>scheduler_tag_ids=\[)(?P<ids>.*)(?P<suffix>\])'.
    'scheduler_tag_ids=[' is identified as the prefix
    'id_1', "id_2"' is identified as the ids and mapped to the e2e ids.
    ']' is identified as the suffix
    the output is scheduler_tag_ids=['id_1, 'id_2']
    :param id_to_e2e_id_mapping: ids will be replaced with e2e ids based on this mapping
    """

    if not id_to_e2e_id_mapping:
        return contractdata

    def replace_ids(match: re.Match):
        # ids potentially commma-separated list, surrounded by quotes and with optional whitespace
        match_groupdict = match.groupdict()
        resource_ids_str = str(match_groupdict["ids"])
        resource_ids_str = (
            resource_ids_str.replace(" ", "").replace('"', "").replace("'", "")
        )
        resource_ids = resource_ids_str.split(",")
        prefix = str(match_groupdict.get("prefix", ""))
        suffix = str(match_groupdict.get("suffix", ""))

        mapped_resource_ids = list()
        for resource_id in resource_ids:
            mapped_tag_id = id_to_e2e_id_mapping.get(resource_id)
            if not mapped_tag_id:
                log.info(
                    f"Did not find mapped {resource_type} id for {resource_id}."
                    f" Using original id"
                )
                mapped_resource_ids.append("'" + resource_id + "'")
            else:
                mapped_resource_ids.append("'" + mapped_tag_id + "'")

        return prefix + ", ".join(mapped_resource_ids) + suffix

    contractdata = re.sub(pattern=id_regex, repl=replace_ids, string=contractdata)

    return contractdata


def replace_workflow_ids_with_e2e_ids(contractdata: str) -> str:
    """
    Replaces the original workflow definition ids inside a smart contract with the run-specific
    ids, so that contracts uploaded by the e2e framework will use the updated workflow versions

    Note: this will only replace instances of workflows being instantiated using kwargs, i.e.
    either "workflow='{wf_name}'" or "workflow="{wf_name}""
    :param contractdata: str, the smart contract code
    :return: str, the updated smart contract code
    """

    return _replace_resource_ids_with_e2e_ids(
        contractdata=contractdata,
        resource_type="workflow_definition_id",
        id_regex=r"(?P<prefix>workflow=)(?P<ids>.*)(?P<suffix>,)",
        id_to_e2e_id_mapping=endtoend.testhandle.workflow_definition_id_mapping,
    )


def replace_schedule_tag_ids_with_e2e_ids(contractdata: str) -> str:
    """
    Replaces the original scheduler tag ids inside a smart contract with the run-specific ids
    :param contractdata: the smart contract code
    :return: the updated smart contract code
    """

    return _replace_resource_ids_with_e2e_ids(
        contractdata=contractdata,
        resource_type="schedule_tag_id",
        id_regex=r"(?P<prefix>scheduler_tag_ids=\[)(?P<ids>.*)(?P<suffix>\])",
        id_to_e2e_id_mapping=endtoend.testhandle.schedule_tag_ids_to_e2e_ids,
    )


def replace_calendar_ids_with_e2e_ids(contractdata: str) -> str:
    """
    Replaces the original calendar ids inside a smart contract with the run-specific ids.
    The calendar id can be found in the code in two scenarios:
    as calendar=["calendar_id"] in the case of a hook '@requires' decorator or
    as calendar_ids=["calendar_id"] in the case of a call to vault.get_calendar_events
    :param contractdata: the smart contract code
    :return: the updated smart contract code
    """

    return _replace_resource_ids_with_e2e_ids(
        contractdata=contractdata,
        resource_type="calendar_id",
        id_regex=r"(?P<prefix>calendar(_ids)?=\[)(?P<ids>.*)(?P<suffix>\])",
        id_to_e2e_id_mapping=endtoend.testhandle.calendar_ids_to_e2e_ids,
    )


def create_account_schedule_tags(
    schedule_tag_ids_to_file_paths: Dict[str, str]
) -> None:
    """
    Creates the required account schedule tags, handling:
    - scenarios where they may already exist
    - creation of unique tag ids per test run to manipulate schedule behaviours without causing
    conflicts between tests.
    - reuse of tag across schedules where possible
    - creation of per-test test_pause_at_timestamp values. If the test_pause_at_timestamp value in
    the tag resource yaml is a dictionary, the value of key-value pair with key 'delta' is fed as
    kwargs to a relativedelta function, which is added to datetime.now() in UTC tz, resulting in an
    offset w.r.t current time. For example, the following yaml would result in a
    test_pause_at_timestamp of 1h before datetime.now():
    ```
        test_pause_at_timestamp:
          delta:
            hours: -1
    ```
    :param tag_ids_to_file_paths: contract schedule tag ids and corresponding resource
    file paths
    """
    log.info("Creating required account schedule tags")

    tags_to_create = {
        (tag_id, schedule_tag_ids_to_file_paths[tag_id]): extract_resource(
            schedule_tag_ids_to_file_paths[tag_id], "account_schedule_tag"
        )
        for tag_id in schedule_tag_ids_to_file_paths
    }

    # the tag ids inside the contract and inside the tag file may differ for test purposes
    for (contract_tag_id, tag_file_path), tag in tags_to_create.items():

        # re-use e2e tags if the same tag definition file is re-used across schedules or tests
        if tag_file_path in endtoend.testhandle.schedule_tag_file_paths_to_e2e_ids:
            e2e_tag_id = endtoend.testhandle.schedule_tag_file_paths_to_e2e_ids[
                tag_file_path
            ]
            endtoend.testhandle.schedule_tag_ids_to_e2e_ids[
                contract_tag_id
            ] = e2e_tag_id
            log.info(f"Reusing tag {e2e_tag_id} for {contract_tag_id}")
            continue

        # no need to map tag ids for unaccelerated tests as they will not manipulate schedules
        if endtoend.testhandle.is_accelerated_test:
            e2e_tag_id = tag["id"] + "_" + str(uuid.uuid4())
        else:
            e2e_tag_id = tag["id"]

        endtoend.testhandle.schedule_tag_ids_to_e2e_ids[contract_tag_id] = e2e_tag_id
        endtoend.testhandle.schedule_tag_file_paths_to_e2e_ids[
            tag_file_path
        ] = e2e_tag_id
        log.info(f"Creating tag {e2e_tag_id} for {contract_tag_id}")

        for time_delta_field in VALID_ACCOUNT_SCHEDULE_TAG_TIME_DELTA_FIELDS:
            timestamp_field = tag.get(time_delta_field)
            if type(timestamp_field) is dict:
                tag[time_delta_field] = (
                    datetime.now(timezone.utc)
                    + relativedelta(**timestamp_field.get("delta", {}))
                ).isoformat()

        try:
            endtoend.core_api_helper.create_account_schedule_tag(
                account_schedule_tag_id=e2e_tag_id,
                description=tag.get("description", ""),
                sends_scheduled_operation_reports=tag.get(
                    "sends_scheduled_operation_reports", False
                ),
                schedule_status_override=tag.get("schedule_status_override"),
                schedule_status_override_start_timestamp=tag.get(
                    "schedule_status_override_start_timestamp",
                ),
                schedule_status_override_end_timestamp=tag.get(
                    "schedule_status_override_end_timestamp"
                ),
                test_pause_at_timestamp=tag.get("test_pause_at_timestamp"),
            )
        except HTTPError as e:
            # This handles cases where the tag has already been created
            if "409 Client Error: Conflict" in e.args[0]:
                existing_tag = (
                    endtoend.endtoend.core_api_helper.batch_get_account_schedule_tags(
                        account_schedule_tag_ids=[e2e_tag_id]
                    ).get(e2e_tag_id)
                )
                if existing_tag != tag:
                    # We could expand this to attempt updates, but it is normally a sign of
                    # incorrect tag_id reuse and should be fixed elsewhere
                    raise ValueError(
                        f"Found existing tag with same id but different attributes."
                        f"\nExisting Tag:\n{existing_tag}\nNew Tag:\n{tag}"
                    )
            else:
                raise e


def create_flag_definitions(flag_definitions: Dict[str, str]) -> None:
    """
    Creates flag definitions, handling scenarios where they may already exist
    :param flag_definitions: list of flag definition ids and corresponding resource file
     paths
    """
    log.info("Creating required flag definitions")

    required_flag_definitions = {
        flag_definition_id: extract_resource(
            file_path=flag_definitions[flag_definition_id],
            resource_type="flag_definition",
        )
        for flag_definition_id in list(flag_definitions.keys())
    }

    flag_definitions_to_create = list()
    # In most cases the definitions will already exist, unless we're on a new environment, or
    # introducing a new definition. We therefore first check if they exist and only create if
    # missing, vs first trying to create, which will mostly fail due to existing definitions
    for flag_definition_id, definition in required_flag_definitions.items():
        try:
            existing_definition = endtoend.core_api_helper.batch_get_flag_definitions(
                ids=[flag_definition_id]
            )[flag_definition_id]
            if not existing_definition.get("is_active", False):
                flag_definitions_to_create.append(definition)

            if existing_definition["is_active"] is False:
                raise ValueError(
                    f"Found inactive flag definition with same id. This cannot currently be "
                    f"reactivated. The contracts may need to be amended to use a new definition."
                    f"\nExisting definition:\n{existing_definition}\nNew definition:\n{definition}"
                )

            if not is_same_flag_definition(definition, existing_definition):
                raise ValueError(
                    f"Found existing flag definition with same id but different attributes."
                    f"\nExisting Tag:\n{existing_definition}\nNew Tag:\n{definition}"
                )
        except HTTPError as e:
            if "404 Client Error: Not Found for url" in e.args[0]:
                log.debug(f"{flag_definition_id} not found in env")
                flag_definitions_to_create.append(definition)
            else:
                raise e

    for flag_definition in flag_definitions_to_create:
        flag_definition_name = flag_definition.get("name")
        log.info(f"Creating flag definition {flag_definition_name}")
        endtoend.core_api_helper.create_flag_definition(
            flag_id=flag_definition_name,
            name=flag_definition_name,
            description=flag_definition.get("description"),
            required_flag_level=flag_definition.get("required_flag_level"),
            flag_visibility=flag_definition.get("flag_visibility"),
        )


def upload_internal_products(internal_products: Iterable[str]) -> None:
    available_products = {
        "TSIDE_ASSET": {
            "path": "internal_accounts/testing_internal_asset_account_contract.py",
            "is_internal": True,
        },
        "TSIDE_LIABILITY": {
            "path": "internal_accounts/testing_internal_liability_account_contract.py",
            "is_internal": True,
        },
    }

    products_to_upload = {}
    for product in internal_products:
        if (
            product in available_products.keys()
            and product
            not in endtoend.testhandle.internal_contract_pid_to_uploaded_pid.keys()
        ):
            products_to_upload[product] = available_products.get(product)
        elif (
            product in endtoend.testhandle.internal_contract_pid_to_uploaded_pid.keys()
        ):
            log.info(f"Product {product} is already uploaded.")
        else:
            raise SetupError(f"Product {product} is not available.")

    upload_contracts(products_to_upload)


def create_required_internal_accounts(
    required_internal_accounts: Dict[str, List[str]]
) -> None:
    """
    Creates the required internal accounts, handling scenarios where they may already exist.
    Ensures that the DUMMY_CONTRA internal account is created even when excluded
    from the required_internal_accounts dict
    :param required_internal_accounts: dict of internal account ids
    """
    log.info("Creating required internal products")

    upload_internal_products(required_internal_accounts.keys())

    log.info("Creating required internal accounts")

    liability_accounts = required_internal_accounts.setdefault(
        "TSIDE_LIABILITY", [DUMMY_CONTRA]
    )
    if DUMMY_CONTRA not in liability_accounts:
        liability_accounts.append(DUMMY_CONTRA)

    internal_accounts_to_create = dict()
    for product, internal_account_ids in required_internal_accounts.items():
        for internal_account_id in internal_account_ids:
            if product not in internal_accounts_to_create.keys():
                internal_accounts_to_create[product] = list()

            tside = "A" if product == "TSIDE_ASSET" else "L"
            e2e_composite_id = "e2e_" + tside + "_" + internal_account_id

            existing_account = {}
            try:
                existing_account = get_internal_account(e2e_composite_id)
            except HTTPError as e:
                if "404 Client Error: Not Found for url" in e.args[0]:
                    log.debug(f"{e2e_composite_id} not found in env")
                else:
                    raise e

            if existing_account.get("id", None):
                endtoend.testhandle.internal_account_id_to_uploaded_id[
                    internal_account_id
                ] = e2e_composite_id
            else:
                internal_accounts_to_create[product].append(internal_account_id)

    for product, internal_account_ids in internal_accounts_to_create.items():
        for internal_account_id in internal_account_ids:
            created_account = create_internal_account(
                account_id=internal_account_id,
                contract=product,
                accounting_tside=product,
            )
            log.info(f'Internal Account {created_account["id"]} created')


def extract_resource(file_path: str, resource_type: str) -> Dict[str, Any]:
    """
    Loads the file at the specified file path, parses the yaml content and returns the
    relevant resource
    """
    if "resources.yaml" in file_path:
        raise ValueError(
            f"Only resource.yaml files are currently supported. "
            f"{file_path} recognised as a resources.yaml file"
        )
    with open(file=file_path, mode="r", encoding="utf-8") as yaml_file:
        yaml_str = yaml_file.read()
    resource = yaml.safe_load(yaml_str)
    payload_yaml = resource["payload"]
    payload = yaml.safe_load(payload_yaml)

    return payload[resource_type]


def is_same_flag_definition(*flag_definitions) -> bool:
    """
    Compares a list of flag definitions, returning True if they match or False otherwise.
    Fields that can't be set on creation are ignored
    Fields that won't impact testing coverage, such as Flag Visibility, are also ignored
    """
    # TODO: Uploading flag defs with unique e2e IDs is being investigated in INC-3420
    # This will likely make this function obsolete so review and remove as appropriate.

    non_create_and_not_pertinent_for_testing_fields = {
        "is_active": None,
        "create_timestamp": None,
        "flag_visibility": None,
    }
    for flag_definition in flag_definitions:
        flag_definition.update(non_create_and_not_pertinent_for_testing_fields)

    return all(
        flag_definition == flag_definitions[0] for flag_definition in flag_definitions
    )


def check_product_version(product_id: str, file_product_data: str):
    """
    Check the Product version against the instance
    used for testing.

    An exception is raised if it is present in the instance at the same version
    but different content.

    :param product_id: The identifier of the product
    :param file_product_data: The parsed product data file
    """
    try:
        product_version_id = get_current_product_version_id(product_id, e2e=False)
        resp = endtoend.core_api_helper.get_product_version(product_version_id, True)
        instance_product_data = resp["code"]
        instance_product_ver = _get_version_identifier(resp["display_version_number"])
    except (HTTPError, KeyError, TypeError):
        log.warning(
            f"{product_id} not found on instance."
            f"Skipping product validation for this smart contract."
        )
        return None

    file_product_ver = _get_file_product_version(file_product_data)
    # If this product is loaded on the instance and at the same version,
    # check the content is the same
    if (
        instance_product_ver == file_product_ver
        and instance_product_data != file_product_data
    ):
        raise ValueError(
            f"{datetime.utcnow()} -"
            f" Instance has different content for {product_id}:"
            f" Version increment may be required"
        )
    else:
        log.info(f"{product_id} {file_product_ver} matches on instance")


def _get_file_product_version(product_data: str) -> str:
    """
    Extracts the version of a product from the file contents
    """
    version_line = re.search(r"version\s=\s'[0-9]+\.[0-9]+\.[0-9]+'", product_data)
    if version_line:
        return version_line.groupdict()["version"]
    raise Exception("Cannot extract version of the smart contract")


def _get_version_identifier(product_version_number: Dict[str, int]) -> str:
    """
    Extracts version identifer as str from the dictionary coming back in batchGet API response.

    :param product_version_number: Example structure:
    {   "major": 1,
        "minor": 1,
        "patch": 1,
        "label": "value1"
        }
    """
    version_id_list = [
        str(product_version_number[part]) for part in ["major", "minor", "patch"]
    ]
    return ".".join(version_id_list)


def convert_account(account_id: str, product_version_id: str) -> Dict[str, Any]:
    """
    Perform an account conversion using account updates
    :param account_id: account id of the account to convert
    :param to_product_id: target product ID to convert account to
    :return: The resulting account update resource
    """

    account_update = {
        "product_version_update": {"product_version_id": product_version_id}
    }
    return endtoend.core_api_helper.create_account_update(account_id, account_update)


def create_calendars(calendars: Dict[str, str]) -> None:
    """
    Creates calendars, handling scenarios where they may already exist
    :param required_calendars: contract calendar ids and their corresponding resource file paths
    """
    log.info("Creating required calendars")

    required_calendars_definitions = {
        calendar_id: extract_resource(
            file_path=calendars[calendar_id],
            resource_type="calendar",
        )
        for calendar_id in calendars
    }

    # In most cases the calendars will already exist, unless we're on a new environment, or
    # introducing a new one. We therefore first check if they exist and only create if
    # missing, vs first trying to create, which will mostly fail due to existing definitions
    for (
        contract_calendar_id,
        calendar_definition,
    ) in required_calendars_definitions.items():

        e2e_calendar_id = _generate_unique_calendar_id(contract_calendar_id)
        calendar_display_name = calendar_definition.get("display_name", "")

        log.info(f"creating calendar with id {e2e_calendar_id}")
        endtoend.core_api_helper.create_calendar(
            calendar_id=e2e_calendar_id,
            is_active=calendar_definition.get("is_active", True),
            display_name=calendar_display_name,
            description=calendar_definition.get("description", ""),
        )

        endtoend.testhandle.calendar_ids_to_e2e_ids[
            contract_calendar_id
        ] = e2e_calendar_id


def _generate_unique_calendar_id(contract_calendar_id: str) -> str:
    """
    Generates a unique calendar id given an original id
    :param contract_calendar_id: the original calendar id
    :return: the unique calendar id
    """
    random_chars = "".join(random.choice(string.ascii_letters) for x in range(10))

    return "e2e_" + contract_calendar_id + "_" + random_chars.upper()


def deactivate_all_calendars():
    for calendar_id in endtoend.testhandle.calendar_ids_to_e2e_ids.values():
        endtoend.core_api_helper.update_calendar(calendar_id, is_active=False)
        log.info(f"Calendar {calendar_id} active status set to false")
