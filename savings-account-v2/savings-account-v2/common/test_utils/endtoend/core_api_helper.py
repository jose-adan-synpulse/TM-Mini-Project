# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# standard libs
import json
import logging
import random
import os
import uuid
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

# common
import common.test_utils.endtoend as endtoend

# third party
from requests import HTTPError

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class AccountStatus(Enum):
    ACCOUNT_STATUS_UNKNOWN = "ACCOUNT_STATUS_UNKNOWN"
    ACCOUNT_STATUS_OPEN = "ACCOUNT_STATUS_OPEN"
    ACCOUNT_STATUS_CLOSED = "ACCOUNT_STATUS_CLOSED"
    ACCOUNT_STATUS_CANCELLED = "ACCOUNT_STATUS_CANCELLED"
    ACCOUNT_STATUS_PENDING_CLOSURE = "ACCOUNT_STATUS_PENDING_CLOSURE"
    ACCOUNT_STATUS_PENDING = "ACCOUNT_STATUS_PENDING"


class CalendarEventStatus(Enum):
    BOTH = "BOTH"
    ONLY_TRUE = "ONLY_TRUE"
    ONLY_FALSE = "ONLY_FALSE"


def create_customer(
    title="CUSTOMER_TITLE_MR",
    first_name="e2eTest",
    middle_name="",
    last_name="Smith",
    dob="1980-12-25",
    gender="CUSTOMER_GENDER_MALE",
    nationality="GB",
    email_address="e2etesting@tm.com",
    mobile_phone_number="+442079460536",
    home_phone_number="+442079460536",
    business_phone_number="+442079460536",
    contact_method="CUSTOMER_CONTACT_METHOD_NONE",
    country_of_residence="GB",
    country_of_taxation="GB",
    accessibility="CUSTOMER_ACCESSIBILITY_AUDIO",
    additional_details=None,
    details=None,
):

    datestr = datetime.now().strftime("%Y%m%d%H%M%S")
    randid = str(random.getrandbits(58))
    cust_id = datestr + randid[len(datestr) :]  # noqa: E203

    default_customer = {
        "request_id": uuid.uuid4().hex,
        "customer": {
            "id": cust_id,
            "status": "CUSTOMER_STATUS_ACTIVE",
            "identifiers": [
                {"identifier_type": "IDENTIFIER_TYPE_USERNAME", "identifier": cust_id}
            ],
            "customer_details": {
                "title": title,
                "first_name": first_name,
                "middle_name": middle_name,
                "last_name": last_name,
                "dob": dob,
                "gender": gender,
                "nationality": nationality,
                "email_address": email_address,
                "mobile_phone_number": mobile_phone_number,
                "home_phone_number": home_phone_number,
                "business_phone_number": business_phone_number,
                "contact_method": contact_method,
                "country_of_residence": country_of_residence,
                "country_of_taxation": country_of_taxation,
                "accessibility": accessibility,
            },
            "additional_details": additional_details,
        },
    }

    customer_details = json.dumps(details if details else default_customer)
    customer = endtoend.helper.send_request(
        "post", "/v1/customers", data=customer_details
    )

    log.info("Customer %s created", customer["id"])
    endtoend.testhandle.customers.append(customer["id"])
    return customer["id"]


def get_customer(customer_id):
    resp = endtoend.helper.send_request("get", "/v1/customers/" + customer_id)
    return resp


def get_existing_test_customer():
    if endtoend.testhandle.customers:
        return endtoend.testhandle.customers[0]
    else:
        return create_customer()


def set_customer_status(customer_id, status):
    post_body = {
        "request_id": uuid.uuid4().hex,
        "customer": {"status": status},
        "update_mask": {"paths": ["status"]},
    }

    resp = endtoend.helper.send_request(
        "put", "/v1/customers/" + customer_id, data=json.dumps(post_body)
    )
    log.info("Customer %s set to %s", customer_id, status)
    return resp


def get_customer_accounts(customer_id):
    resp = endtoend.helper.send_request(
        "get",
        "/v1/accounts",
        params={"stakeholder_id": customer_id, "page_size": "100"},
    )

    return resp["accounts"]


def get_customer_addresses(customer_id):
    body = {"customer_id": customer_id, "page_size": "1000", "include_previous": "True"}

    resp = endtoend.helper.send_request("get", "/v1/customer-addresses", params=body)

    # A list of customer addresses, ordered by descending creation time.
    return resp["customer_addresses"]


def create_payment_device(routing_info, status="PAYMENT_DEVICE_STATUS_ACTIVE"):
    post_body = {
        "payment_device": {"routing_info": routing_info, "status": status},
        "request_id": uuid.uuid4().hex,
    }

    resp = endtoend.helper.send_request(
        "post", "/v1/payment-devices", data=json.dumps(post_body)
    )

    return resp


def create_payment_device_link(
    payment_device_id,
    account_id,
    token=None,
    status="PAYMENT_DEVICE_LINK_STATUS_ACTIVE",
):

    post_body = {
        "payment_device_link": {
            "token": token,
            "payment_device_id": payment_device_id,
            "account_id": account_id,
            "status": status,
        },
        "request_id": uuid.uuid4().hex,
    }

    resp = endtoend.helper.send_request(
        "post", "/v1/payment-device-links", data=json.dumps(post_body)
    )

    return resp


def get_payment_device_links(
    tokens=None,
    payment_device_ids=None,
    account_ids=None,
    effective_timestamp=None,
    include_inactive=None,
):
    # Returns a list of payment device links, or an empty list if none found.

    resp = endtoend.helper.send_request(
        "get",
        "/v1/payment-device-links",
        params={
            "tokens": tokens,
            "payment_device_ids": payment_device_ids,
            "account_ids": account_ids,
            "effective_timestamp": effective_timestamp,
            "include_inactive": include_inactive,
        },
    )

    return resp["payment_device_links"]


def get_payment_device(payment_device_ids):
    # If this ID doesn't exist, Vault will throw an error

    resp = endtoend.helper.send_request(
        "get", "/v1/payment-devices:batchGet", params={"ids": payment_device_ids}
    )

    return resp["payment_devices"][payment_device_ids]


def get_uk_acc_num_and_sort_code(account_id):
    pd_link = get_payment_device_links(account_ids=account_id)

    if len(pd_link) == 0:
        raise NameError(
            "No payment device link found for " "account {}".format(account_id)
        )

    # todo: Search through all pd_links
    pd = get_payment_device(pd_link[0]["payment_device_id"])

    if all(word in pd["routing_info"] for word in ["account_number", "bank_id"]):
        return pd["routing_info"]["account_number"], pd["routing_info"]["bank_id"]
    else:
        raise NameError(
            "No account number or sort code found for account "
            "{}. Has it been set up with UK routing info?".format(account_id)
        )


def create_flag_definition(
    flag_id: str,
    name: str = "",
    description: str = "",
    required_flag_level: str = "FLAG_LEVEL_ACCOUNT",
    flag_visibility: str = "FLAG_VISIBILITY_CONTRACT",
) -> Dict[str, str]:

    name = name or flag_id
    description = description or flag_id

    post_body = {
        "request_id": uuid.uuid4().hex,
        "flag_definition": {
            "id": flag_id,
            "name": name,
            "description": description,
            "required_flag_level": required_flag_level,
            "flag_visibility": flag_visibility,
        },
    }

    resp = endtoend.helper.send_request(
        "post", "/v1/flag-definitions", data=json.dumps(post_body)
    )
    log.info(f"Flag created: {description}")
    return resp


def list_flag_definitions(
    flag_visibility: str = "FLAG_VISIBILITY_CONTRACT",
    flag_levels: List[str] = None,
    include_inactive: str = "true",
) -> List[Dict[str, Any]]:
    body = {
        "flag_visibility_level": flag_visibility,
        "flag_levels": flag_levels or ["FLAG_LEVEL_ACCOUNT", "FLAG_LEVEL_CUSTOMER"],
        "include_inactive": include_inactive,
    }
    resp = endtoend.helper.list_resources("flag-definitions", params=body)
    return resp


def batch_get_flag_definitions(ids: List[str]) -> Dict[str, Dict[str, str]]:

    return endtoend.helper.send_request(
        "get", "/v1/flag-definitions:batchGet", params={"ids": ids}
    )["flag_definitions"]


def create_flag(
    flag_name: str,
    account_id: str = None,
    customer_id: str = None,
    payment_device_id: str = None,
    description: str = None,
):

    description = description or flag_name
    if account_id:
        target = "account_id"
        target_id = account_id
    elif customer_id:
        target = "customer_id"
        target_id = customer_id
    elif payment_device_id:
        target = "payment_device_id"
        target_id = payment_device_id
    else:
        raise NameError("No target has been specified so flag can not be applied!")

    post_body = {
        "request_id": uuid.uuid4().hex,
        "flag": {
            "flag_definition_id": flag_name,
            "description": description,
            target: target_id,
        },
    }

    resp = endtoend.helper.send_request("post", "/v1/flags", data=json.dumps(post_body))
    log.info(f"Flag applied for account {account_id}: {description}")
    return resp


def remove_flag(flag_id: str) -> Dict[str, str]:
    put_body = {
        "request_id": uuid.uuid4().hex,
        "flag": {
            "is_active": False,
        },
        "update_mask": {"paths": ["is_active"]},
    }
    resp = endtoend.helper.send_request(
        "put", f"/v1/flags/{flag_id}", data=json.dumps(put_body)
    )
    log.info(f'Flag {flag_id} {resp["description"]} removed')
    return resp


def get_flag(flag_name: str, account_ids: List[str] = None) -> List[Dict[str, Any]]:
    body = {"flag_definition_id": flag_name, "account_ids": account_ids or []}
    resp = endtoend.helper.list_resources("flags", params=body)
    return resp


def create_restriction_set_definition_version(
    restriction_id, restriction_type, restriction_level, description=None
):

    description = description or restriction_id

    post_body = {
        "request_id": uuid.uuid4().hex,
        "restriction_set_definition_version": {
            "restriction_set_definition_id": restriction_id,
            "description": description,
            "restriction_definitions": [
                {
                    "restriction_type": restriction_type,
                    "required_restriction_levels": restriction_level,
                }
            ],
        },
    }

    resp = endtoend.helper.send_request(
        "post",
        "/v1/restriction-set-definition/blocking_test/versions",
        data=json.dumps(post_body),
    )
    log.info(f"Restriction created: {description}")
    return resp


def create_restriction_set(account_id, restriction_id, name=None, description=None):

    name = name or restriction_id
    description = description or restriction_id

    post_body = {
        "request_id": uuid.uuid4().hex,
        "restriction_set": {
            "restriction_set_definition_id": restriction_id,
            "name": name,
            "description": description,
            "restriction_set_parameters": {},
            "account_id": account_id,
        },
    }

    resp = endtoend.helper.send_request(
        "post", "/v1/restriction-sets", data=json.dumps(post_body)
    )
    log.info("Restriction applied to account %s preventing debits", account_id)
    return resp


def remove_restriction_set(account_id, restriction_set_id):
    resp = update_restriction_set(restriction_set_id, "is_active", False)
    log.info(f"Restriction set removed from account {account_id} preventing debits")
    return resp


def update_restriction_set(restriction_set_id, update_field, update_value):
    post_body = {
        "request_id": uuid.uuid4().hex,
        "restriction_set": {"id": restriction_set_id, update_field: update_value},
        "update_mask": {"paths": [update_field]},
    }

    resp = endtoend.helper.send_request(
        "put", "/v1/restriction-sets/" + restriction_set_id, data=json.dumps(post_body)
    )
    log.info(
        f"Restriction set {restriction_set_id} updated: {update_field} set to {update_value}"
    )
    return resp


def get_account_schedule_assocs(account_id: str) -> List[Dict[str, Any]]:
    body = {
        "account_id": account_id,
    }
    resp = endtoend.helper.list_resources("account-schedule-assocs", params=body)

    # A list of account to schedule associations
    return resp


def get_schedules(schedule_ids):
    body = {"ids": schedule_ids}

    resp = endtoend.helper.send_request("get", "/v1/schedules:batchGet", params=body)

    # A dict of schedule_id to schedule objects
    return resp["schedules"]


def get_jobs(schedule_id: str) -> List[Dict[str, Any]]:
    """
    Gets all the jobs with the specified schedule_id
    :param schedule_id: id for filterling which jobs to retrieved.
    ...
    :return: List of schedules with the specified schedule_id else
    return empty list
    """
    body = {"schedule_id": schedule_id}
    result = endtoend.helper.list_resources("jobs", params=body)
    return result


def get_account_schedules(account_id, invalid_statuses: Optional[List[str]] = None):
    if invalid_statuses != []:
        invalid_statuses = invalid_statuses or ["SCHEDULE_STATUS_DISABLED"]
    account_schedule_assocs = get_account_schedule_assocs(account_id)

    if not account_schedule_assocs:
        return {}
    account_schedule_ids = [assoc["schedule_id"] for assoc in account_schedule_assocs]

    response_account_schedules = get_schedules(account_schedule_ids)

    account_schedules = {}
    for _, schedule_details in response_account_schedules.items():
        # Schedule display name is of format "<EVENT_NAME> for <ACCOUNT_ID>"
        if schedule_details["status"] not in invalid_statuses and schedule_details[
            "display_name"
        ].endswith(f" for {account_id}"):
            account_schedule_name = schedule_details["display_name"].replace(
                f" for {account_id}", ""
            )
            account_schedules[account_schedule_name] = schedule_details

    # A dict of schedule event_names to their schedule objects
    return account_schedules


def get_account_derived_parameters(account_id: str, effective_timestamp: str = ""):
    body = {"fields_to_include": ["INCLUDE_FIELD_DERIVED_INSTANCE_PARAM_VALS"]}
    if effective_timestamp:
        body.update({"instance_param_vals_effective_timestamp": effective_timestamp})

    resp = endtoend.helper.send_request(
        "get", f"/v1/accounts/{account_id}", params=body
    )

    return resp["derived_instance_param_vals"]


def get_balances(
    account_id: str,
    from_value_time: datetime = None,
    to_value_time: datetime = None,
    exclude_starting_balance: bool = False,
    live: bool = True,
    posting_instruction_batch_id: str = "",
) -> List[Dict[str, str]]:
    """
    Gets balances for a given account
    :param account_id: the account to retrieve
    :param from_value_time: Optional value time to retrieve from. Ignored if live=True
    :param to_value_time: Optional value time to retrieve up until. Ignored if live=True
    :param exclude_starting_balance: if True the balances before from_value_time are excluded.
    Ignored if live=True
    :param live: set to True if for live balances only, or False to also get historical balances
    :param posting_instruction_batch_id: The posting instruction batch ID that initially created
    the balance
    :return: the list of balances
    """
    params = {
        "account_id": account_id,
        "live": live,
        "posting_instruction_batch_id": posting_instruction_batch_id,
    }
    if not live:
        if from_value_time:
            params.update(
                {
                    "time_range.from_value_time": from_value_time.astimezone(
                        timezone.utc
                    ).isoformat()
                }
            )
        if to_value_time:
            params.update(
                {
                    "time_range.to_value_time": to_value_time.astimezone(
                        timezone.utc
                    ).isoformat()
                }
            )
        if exclude_starting_balance:
            params.update(
                {"time_range.exclude_starting_balance": exclude_starting_balance}
            )

    resp = endtoend.helper.list_resources("balances", params)

    return resp


def get_account_update(account_update_id: str) -> Dict[str, Any]:
    """
    Retrieve a specific account update by its id
    :param account_update_id: id of the account update to retrieve
    :return: the account update resource
    """

    resp = endtoend.helper.send_request(
        "get", f"/v1/account-updates/{account_update_id}"
    )

    return resp


def get_account_updates(
    account_id: str, statuses: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Get a list of account updates for a given account
    :param account_id: account id for the account to get updates for
    :param statuses: statuses of account updates to filter on. Optional.
    :return: list of account updates matching the criteria
    """

    params = {"account_id": account_id, "statuses": statuses}
    return endtoend.helper.list_resources("account-updates", params)


def get_account_updates_by_type(
    account_id: str, update_types: List[str], statuses: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Gets a list of account updates and filters by type
    :param account_id: the account id to get account updates for
    :param update_types: the list of account update types we want to filter for (not handled in API)
    :param statuses: the list of account update statuses we want to filter for (handled in API)
    :return: List of account updates
    """

    account_updates = get_account_updates(account_id, statuses)
    account_updates_by_type = [
        account_update
        for account_update in account_updates
        for update_type in update_types
        if update_type in account_update
    ]
    return account_updates_by_type


def get_product_version(product_version_id: str, include_code: bool = False) -> Dict:
    """
    Fetches product version from instance.

    :param product_version_id: Instance version id of the product, not the id found in the sc file
    :param include_code: Specifies whether raw code needs to be included in response
    """
    view = ["PRODUCT_VERSION_VIEW_INCLUDE_CODE"] if include_code else []
    params = {"ids": product_version_id, "view": view}
    resp = endtoend.helper.send_request(
        "get", "/v1/product-versions:batchGet", params=params
    )
    return resp["product_versions"][product_version_id]


def create_account_update(
    account_id: str,
    account_update: Dict[str, Dict[str, Any]],
    account_update_id: str = "",
) -> Dict[str, Any]:
    """

    :param account_id: account id of the account to update
    :param account_update: Dict where the key is the desired account update (i.e.
    instance_param_vals_update, product_version_update, activation_update, closure_update) and the
    value is the Dict with the required parameters for the account update type. For example:
    {
        'instance_param_vals_update': {
            'instance_param_vals': {
                'KEY': 'value1'
            }
        }
    }
    :param account_update_id: optional account update id to use. Randomly generated by service if
    omitted
    :return: The resulting account update resource
    """

    body = {
        "request_id": uuid.uuid4().hex,
        "account_update": {
            "id": account_update_id,
            "account_id": account_id,
            **account_update,
        },
    }
    jsonbody = json.dumps(body)
    resp = endtoend.helper.send_request("post", "/v1/account-updates", data=jsonbody)
    log.info(f"Account update {account_update} created")
    return resp


def create_closure_update(account_id: str) -> Dict[str, Any]:
    """
    Creates an account update to re-run the close_code hook once the account status is already
    'ACCOUNT_STATUS_PENDING_CLOSURE'
    :param account_id: the account id of the account to update
    :return: The resulting account update resource
    """
    account_update = {"closure_update": {}}
    return create_account_update(account_id, account_update)


def update_account_instance_parameters(
    account_id: str, instance_param_vals: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Creates an account update to update specified instance parameters to the specified values
    :param account_id: the account id of the account to update
    :param instance_param_vals: dictionary of instance parameter names to updated values
    :return: The resulting account update resource
    """
    account_update = {
        "instance_param_vals_update": {"instance_param_vals": instance_param_vals}
    }
    return create_account_update(account_id, account_update)


def update_account(account_id: str, status: AccountStatus) -> Dict[str, Any]:
    """
    Update an account
    :param account_id: account id of the account to update
    :param status: new account status
    :return: the updated account
    """
    body = {
        "request_id": str(uuid.uuid4()),
        "account": {"status": status.value},
        "update_mask": {"paths": ["status"]},
    }
    body = json.dumps(body)
    resp = endtoend.helper.send_request("put", "/v1/accounts/" + account_id, data=body)
    return resp


def create_product_version(
    request_id: str,
    code: str,
    product_id: str,
    supported_denominations: List[str],
    tags: List[str] = None,
    params: List[Any] = None,
    is_internal: bool = False,
    migration_strategy: str = "PRODUCT_VERSION_MIGRATION_STRATEGY_UNKNOWN",
    contract_properties: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Creates a product version by using the core api endpoint
    :param request_id: str, unique string ID that is used to ensure the request is idempotent
    :param code: str, the smart contract code
    :param product_id: str, the ID of the product we want to create
    :param supported_denominations: List[str], the denominations supported by this product version
    :param tags: List[str], tags for the product version
    :param params: List[object], the parameter values for the product version
    :param is_internal: bool, denotes if the product being uploaded is an internal product or not
    :param migration_strategy: str, the migration strategy for applying the new version
    :param contract_properties: Dict[str, object], the contract specific property values
    :return: Dict[str, object], return value of core api call
    """
    contract_properties = contract_properties or {}
    display_name = contract_properties.get("display_name", "")
    if is_internal:
        migration_strategy = "PRODUCT_VERSION_MIGRATION_STRATEGY_NEW_PRODUCT"

    post_body = {
        # ProductVersions are immutable, so we can use that to simply return an already created
        # ProductVersion.
        "request_id": request_id,
        "product_version": {
            "product_id": product_id,
            "code": code,
            "supported_denominations": supported_denominations,
            "params": params,
            "tags": tags or [],
            "display_name": display_name,
            "description": "",
            "summary": "",
        },
        "is_internal": is_internal,
        "migration_strategy": migration_strategy,
    }

    post_body = json.dumps(post_body)

    resp = endtoend.helper.send_request("post", "/v1/product-versions", data=post_body)

    return resp


def create_account_schedule_tag(
    account_schedule_tag_id: str,
    description: str = "",
    sends_scheduled_operation_reports: bool = True,
    schedule_status_override: str = "ACCOUNT_SCHEDULE_TAG_SCHEDULE_STATUS_OVERRIDE_NO_OVERRIDE",
    schedule_status_override_start_timestamp: Optional[str] = None,
    schedule_status_override_end_timestamp: Optional[str] = None,
    test_pause_at_timestamp: Optional[str] = None,
) -> Dict[str, str]:

    post_body = {
        "request_id": str(uuid.uuid4()),
        "account_schedule_tag": {
            "id": account_schedule_tag_id,
            "description": description,
            "sends_scheduled_operation_reports": sends_scheduled_operation_reports,
            "schedule_status_override": schedule_status_override,
            "schedule_status_override_start_timestamp": schedule_status_override_start_timestamp,
            "schedule_status_override_end_timestamp": schedule_status_override_end_timestamp,
            "test_pause_at_timestamp": test_pause_at_timestamp,
        },
    }

    post_body = json.dumps(post_body)

    resp = endtoend.helper.send_request(
        "post", "/v1/account-schedule-tags", data=post_body
    )

    return resp


def list_account_schedule_tags(result_limit: int) -> List[Dict[str, str]]:

    return endtoend.helper.list_resources(
        "account-schedule-tags", result_limit=result_limit
    )


def batch_get_account_schedule_tags(
    account_schedule_tag_ids: List[str],
) -> Dict[str, Dict[str, str]]:

    return endtoend.helper.send_request(
        "get",
        "/v1/account-schedule-tags:batchGet",
        params={"ids": account_schedule_tag_ids},
    )["account_schedule_tags"]


def update_account_schedule_tag(
    account_schedule_tag_id: str,
    schedule_status_override: Optional[str] = None,
    schedule_status_override_start_timestamp: Optional[str] = None,
    schedule_status_override_end_timestamp: Optional[str] = None,
    test_pause_at_timestamp: Optional[str] = None,
) -> Dict[str, str]:

    update_mask_paths = list()
    account_schedule_tag = dict()

    # status, start timestamp and end timestamp must all be set together
    if schedule_status_override:
        update_mask_paths.extend(
            [
                "schedule_status_override",
                "schedule_status_override_start_timestamp",
                "schedule_status_override_end_timestamp",
            ]
        )
        account_schedule_tag.update(
            {
                "schedule_status_override": schedule_status_override,
                "schedule_status_override_start_timestamp": (
                    schedule_status_override_start_timestamp
                ),
                "schedule_status_override_end_timestamp": schedule_status_override_end_timestamp,
            }
        )

    if test_pause_at_timestamp:
        update_mask_paths.append("test_pause_at_timestamp")
        account_schedule_tag["test_pause_at_timestamp"] = test_pause_at_timestamp

    body = json.dumps(
        {
            "request_id": uuid.uuid4().hex,
            "account_schedule_tag": account_schedule_tag,
            "update_mask": {"paths": update_mask_paths},
        }
    )

    return endtoend.helper.send_request(
        "put", "/v1/account-schedule-tags/" + account_schedule_tag_id, data=body
    )


def get_calendar_events(
    calendar_ids: Optional[List[str]] = None,
    calendar_event_names: Optional[List[str]] = None,
    calendar_timestamp_from: str = "",
    calendar_timestamp_to: str = "",
    is_active: bool = True,
    active_calendar_event: Optional[CalendarEventStatus] = None,
) -> List[Dict[str, Any]]:

    active_calendar_event = active_calendar_event or CalendarEventStatus.ONLY_TRUE

    body = {
        "calendar_ids": calendar_ids or [],
        "calendar_event_names": calendar_event_names or [],
        "calendar_timestamp_range.from": calendar_timestamp_from,
        "calendar_timestamp_range.to": calendar_timestamp_to,
        "is_active": str(is_active).lower(),
        "active_calendar_event": active_calendar_event.value,
    }
    resp = endtoend.helper.list_resources("calendar-event", params=body)

    return resp


def create_calendar_event(
    event_id: str,
    calendar_id: str,
    name: str,
    is_active: bool,
    start_timestamp: datetime,
    end_timestamp: datetime,
):
    post_body = {
        "request_id": uuid.uuid4().hex,
        "calendar_event": {
            "id": event_id,
            "calendar_id": calendar_id,
            "name": name,
            "is_active": is_active,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
        },
    }

    resp = endtoend.helper.send_request(
        "post", "/v1/calendar-event", data=json.dumps(post_body)
    )

    return resp


def list_calendars(
    order_by: str = "ORDER_BY_CREATE_TIMESTAMP_ASC",
    name_pattern_match_pattern: str = None,
    name_pattern_match_match_type: str = "MATCH_TYPE_UNKNOWN",
) -> List[Dict[str, Any]]:
    body = {
        "order_by": order_by,
        "name_pattern_match.pattern": name_pattern_match_pattern,
        "name_pattern_match.match_type": name_pattern_match_match_type,
    }
    resp = endtoend.helper.list_resources("calendars", params=body)

    return resp


def create_calendar(
    calendar_id: str,
    is_active: bool = False,
    display_name: str = "",
    description: str = "",
) -> Dict[str, str]:

    display_name = display_name or calendar_id
    description = description or calendar_id

    post_body = {
        "request_id": uuid.uuid4().hex,
        "calendar": {
            "id": calendar_id,
            "is_active": is_active,
            "display_name": display_name,
            "description": description,
        },
    }

    resp = endtoend.helper.send_request(
        "post", "/v1/calendar", data=json.dumps(post_body)
    )

    return resp


def update_calendar(
    calendar_id: str,
    is_active: bool = None,
    display_name: str = None,
    description: str = None,
) -> Dict[str, str]:

    updated_fields = {}
    if is_active is not None:
        updated_fields["is_active"] = is_active
    if display_name is not None:
        updated_fields["display_name"] = display_name
    if description is not None:
        updated_fields["description"] = description

    post_body = {
        "request_id": uuid.uuid4().hex,
        "calendar": updated_fields,
        "update_mask": {"paths": list(updated_fields.keys())},
    }

    resp = endtoend.helper.send_request(
        "put", f"/v1/calendar/{calendar_id}:updateDetails", data=json.dumps(post_body)
    )

    return resp


def get_contract_modules() -> List[Dict[str, Any]]:

    resp = endtoend.helper.list_resources("contract-modules", params=None)

    return resp


def get_contract_module_versions(
    contract_module_id: str = "",
) -> List[Dict[str, Any]]:

    body = {
        "contract_module_id": contract_module_id,
    }
    resp = endtoend.helper.list_resources(
        "contract-module-versions", params=body, page_size=10
    )

    return resp


def get_smart_contract_module_version_links(
    contract_version_id: str,
) -> List[Dict[str, Any]]:

    resp = endtoend.helper.list_resources(
        "smart-contract-module-versions-links",
        params={"smart_contract_version_ids": contract_version_id},
    )

    return resp


def get_postings_api_client(
    client_id: str,
) -> Dict[str, str]:
    return endtoend.helper.send_request("get", "/v1/postings-api-clients/" + client_id)


def create_postings_api_client(
    request_id: str,
    client_id: str,
    response_topic: str,
) -> Dict[str, str]:
    post_body = {
        "request_id": request_id,
        "postings_api_client": {
            "id": client_id,
            "response_topic": response_topic,
        },
    }

    return endtoend.helper.send_request(
        "post", "/v1/postings-api-clients", data=json.dumps(post_body)
    )


def init_postings_api_client(
    client_id: str, response_topic: str, timeout: int = 5
) -> Dict[str, str]:
    """
    Postings API client can be missing on the target instance (i.e. bootstrap job as part of DR)
    so ensure it's created if it cannot be found.
    """
    for i in range(timeout):
        try:
            return get_postings_api_client(client_id)
        except HTTPError as e:
            if "404" not in e.args[0]:
                if i < timeout:
                    time.sleep(1)
                    continue
                raise HTTPError(
                    "Unexpected error when trying to connect to endpoint /v1/postings-api-clients"
                ) from e

            log.info(
                "Could not find existing Postings API Client with ID: %s."
                "Creating new Postings API Client with above ID.",
                client_id,
            )
            return create_postings_api_client(
                request_id=str(uuid.uuid4()),
                client_id=client_id,
                response_topic=response_topic,
            )
