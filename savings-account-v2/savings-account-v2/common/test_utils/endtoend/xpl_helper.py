# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
import os
import uuid
import json
from common.test_utils.endtoend.helper import send_request
import logging

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def create_payee(
    created_by_customer_id,
    account_id,
    payee_label,
    default_payment_reference,
    beneficiary_name,
    account_number,
    sort_code,
):

    post_body = {
        "request_id": uuid.uuid4().hex,
        "payee": {
            "created_by_customer_id": created_by_customer_id,
            "account_id": account_id,
            "payee_label": payee_label,
            "default_payment_reference": default_payment_reference,
            "uk_bank_identifier": {
                "beneficiary_name": beneficiary_name,
                "account_number": account_number,
                "sort_code": sort_code,
            },
        },
    }

    post_body = json.dumps(post_body)

    res = send_request("post", "/v1/payees", data=post_body)

    return res


def list_payees(customer_id, account_ids=None, include_deleted=None):
    get_body = {
        "customer_id": customer_id,
        "account_ids": account_ids,
        "include_deleted": include_deleted,
        "page_size": "100",
    }

    resp = send_request("get", "/v1/payees", params=get_body)

    return resp["payees"]


def get_payee(payee_id):
    get_body = {"ids": [payee_id]}
    resp = send_request("get", "/v1/payees:batchGet", params=get_body)

    return next(iter(resp["payees"].values()))
