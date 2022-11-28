# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# standard libs
import json
import logging
import os
from datetime import datetime, timedelta
from functools import partial, wraps
from time import sleep
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

# third party
import requests

# common
import common.test_utils.endtoend as endtoend
from common.vault.environment import load_environments

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

CONFIG_FILE = "common/test_utils/endtoend/config.json"
WORKFLOW_ENDPOINTS = ["workflow", "policies", "task", "ticket"]
XPL_ENDPOINTS = ["payees", "immediate", "transactions"]
DATA_LOADER_ENDPOINTS = ["dependency-groups", "resource-batches", "resources"]
PAYMENTS_HUB_ENDPOINTS = ["mandates", "bank-accounts", "bbans", "scheme-messages"]
PROMETHEUS_ENDPOINTS = ["query", "query_range"]


class SetupError(Exception):
    """Raise exception if test setup is incorrect"""

    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class TestInstance:
    def __init__(self):
        # uploaded e2e-specific workflow definition id to uploaded e2e-specific workflow definition
        # version id
        # populated by the test framework
        # used for teardown purposes
        # e.g. {"CLOSE_US_ACCOUNT_e2e_GLAJAILTJT": "1.0.0,CLOSE_US_ACCOUNT_e2e_GLAJAILTJT"}
        self.uploaded_workflows: Dict[str, str] = {}
        # The original workflow definition ids to uploaded e2e-specific workflow definition ids
        # populated by the framework
        # e.g. {"CLOSE_US_ACCOUNT": "CLOSE_US_ACCOUNT_e2e_GLAJAILTJT"}
        self.workflow_definition_id_mapping: Dict[str, str] = {}
        # customer ids for customers created directly by this TestInstance (e.g. exc via workflows)
        # populated by the test framework
        # used for teardown purposes
        self.customers: List[str] = []
        # plan ids for plans created directly by this TestInstance (e.g. exc via workflows)
        # used for teardown purposes
        self.plans: List[str] = []
        # original product id to uploaded e2e-specific product id
        # populated by the test framework
        # e.g. {"current_account": "e2e_current_account_asd213hjh"}
        self.contract_pid_to_uploaded_pid: Dict[str, str] = {}
        # original internal product id to uploaded e2e-specific product id
        # populated by the test framework
        # e.g. {"TSIDE_LIABILITY': "e2e_TSIDE_LIABILITY_89787230b4030eeb5b4284d473f31ace"}
        self.internal_contract_pid_to_uploaded_pid: Dict[str, str] = {}
        # Supervisor contract display name to supervisor contract version id
        # populated by the test framework
        # e.g. {"us_products_supervisor": "BwrqXkvhfS"}
        self.supervisorcontract_name_to_id: Dict[str, str] = {}
        # Contract module alias to version id
        # populated by the test framework
        # e.g. {"interest_module": "BwrqXkvhfS"}
        self.contract_module_version_name_to_id: Dict[str, str] = {}
        # account ids for accounts created directly by this TestInstance (e.g. exc via workfows)
        # used for teardown purposes
        self.accounts: Set[str] = set()
        # original internal account id to uploaded e2e-specific account id. The standalone A/L
        # after the e2e_ prefix indicates whether it is an asset or liability account
        # populated by the test framework
        # e.g. {"ACCRUED_INT_RECEIVABLE": "e2e_A_ACCRUED_INT_RECEIVABLE"}
        self.internal_account_id_to_uploaded_id: Dict[str, str] = {}
        # kafka topics to corresponding kafka consumers
        # populated by the test framework
        self.kafka_consumers: Dict[str, object] = {}
        # kafka producer for general use
        # populated by the test framework
        self.kafka_producer: object = None
        # determines whether kafka helpers will be used, where available
        self.use_kafka: bool = True
        # Product id to attributes. Used by test framework to determine which contracts to create
        # as part of setup
        # Populated by the test writer in each test class
        # Attribute dictionary must include k-v pairs:
        # - key: "path", value: file path to the contract file
        # - key: "template_params", value: dictionary of template param names and values
        # e.g. {
        #    "easy_access_saver": {
        #        "path": "library/easy_access_saver/contracts/easy_access_saver.py",
        #        "template_params": {
        #            "param_name": "value"
        #        },
        #    }
        # }
        self.CONTRACTS: Dict[str, Dict[str, Union[str, Dict[str, str]]]] = {}
        # Contract module id to attributes. Used by test framework to determine which contract
        # modules to create as part of setup
        # Populated by the test writer in each test class
        # Attribute dictionary must include k-v pairs:
        # - key: "path", value: file path to the contract module file
        # Optional attributes:
        # - key: "display_name", value: contract module display name
        # e.g. {
        #   "math_module": {"path": "library/common/contract_modules/math_module.py",
        #                   "display_name": "math_module"}
        # }
        self.CONTRACT_MODULES: Dict[str, Dict[str, str]] = {}
        # Supervisor contract id to attributes. Used by test framework to determine which supervisor
        # contracts to create as part of setup
        # Populated by the test writer in each test class
        # Attribute dictionary must include k-v pairs:
        # - key: "path", value: file path to the supervisor contract file
        # e.g. {
        #    "offset_supervisor_contract": {
        #       "path": "library/mortgage/contracts/offset_supervisor_contract.py"
        #    }
        # }
        self.SUPERVISORCONTRACTS: Dict[str, Dict[str, str]] = {}
        # Workflow definition id to workflow definition file path. Used by test framework to
        # determine which workflow definitions to create as part of setup
        # Populated by the test writer in each test class
        # e.g. {
        #    "APPLY_FOR_TIME_DEPOSIT": (
        #        "library/time_deposit/workflows/apply_for_time_deposit.yaml"
        #    ),
        # }
        self.WORKFLOWS: Dict[str, str] = {}
        # Account schedule tag id to account schedule tag file path. Used by test framework to
        # determine which tags to create as part of setup
        # Populated by the test writer in each test class except for accelerated tests where this
        # is handled via the set_paused_tags decorator
        # e.g. {
        #     "TIME_DEPOSIT_ACCRUE_INTEREST": (
        #         "library/time_deposit/account_schedule_tags/"
        #         "time_deposit_accrue_interest.resource.yaml"
        #     )
        # }
        self.ACCOUNT_SCHEDULE_TAGS: Dict[str, str] = {}
        # flag definition id to flag definition file path
        # e.g. {
        #   "CURRENT_ACCOUNT_TIER_UPPER": (
        #       "library/current_account/flag_definitions/current_account_tier_upper.resource.yaml"
        #   ),
        # }
        self.FLAG_DEFINITIONS: Dict[str, str] = {}
        # Maps TSIDE to a list of internal account ids. Used by test framework to determine which
        # internal products and accounts to create as part of setup.
        # Populated by the test writer in each test class
        # Valid keys are "TSIDE_ASSET" and "TSIDE_LIABILITY"
        # e.g.  "TSIDE_ASSET": ["ACCRUED_INTEREST_RECEIVABLE_ACCOUNT", "INTEREST_PAID_ACCOUNT"],
        self.TSIDE_TO_INTERNAL_ACCOUNT_ID: Dict[str, List[str]] = {}
        # Original account schedule tag id to uploaded e2e-specific id
        # Populated by the framework
        # e.g. {"CREDIT_CARD_ANNUAL_FEE_AST":
        #  "CREDIT_CARD_ANNUAL_FEE_AST_9a87216025c096c38d31960ec5314e2d"}
        self.schedule_tag_ids_to_e2e_ids: Dict[str, str] = {}
        # Account schedule tag file path to uploaded e2e-specific id
        # Populated by the framework
        # e.g. {
        #     ("library/time_deposit/account_schedule_tags/
        #      time_deposit_accrue_interest.resource.yaml"):
        #  "TIME_DEPOSIT_ACCRUE_INTEREST_AST_9a87216025c096c38d31960ec5314e2d"
        # }
        self.schedule_tag_file_paths_to_e2e_ids = {}
        # Calendar ids to calendar definition file path
        # Populated by the test writer
        # e.g. {
        #     "TIME_DEPOSIT_BANK_HOLIDAY": (
        #         "library/time_deposit/calendars/time_deposit_bank_holiday.resource.yaml"
        #     )
        # }
        self.CALENDARS = {}
        # Calendar id to uploaded e2e-specific id
        # Populated by the framework
        # e.g. {
        #     "TIME_DEPOSIT_BANK_HOLIDAY": "e2e_TIME_DEPOSIT_BANK_HOLIDAY_BwrqXkvhfS"
        # }
        self.calendar_ids_to_e2e_ids: Dict[str, str] = {}
        # The default internal account id, used by the framework for certain features like zeroing
        # customer account balances before closure as part of teardown
        self.internal_account: str = "1"
        # A session to minimise the number of new connections opened by a given instance
        self.session: requests.Session = requests.Session()
        # environment names to environment properties. See the config.json for an example
        default_environment, self.available_environments = load_environments(
            CONFIG_FILE
        )
        self.environment = default_environment.name

        # Indicates whether an instance is used for accelerated tests. This is used to optimise
        # setup as accelerated tests result in on-the-fly contract modification as we must create
        # unique account schedule tags per test
        self.is_accelerated_test: bool = False
        # Whether to check version numbers in the deployment file and test instance and
        # fail if there's a mismatch
        self.do_version_check: bool = False
        # Number of seconds delay between one schedule tag pause update and the next
        # This is needed to ensure the results can be distinguished
        # 120 secs should be min because the averaging window on graphs is typically 2min
        self.paused_schedule_tag_delay: int = 600


def setup_environments():
    args, _ = endtoend.extract_args()
    if args.environment:
        endtoend.testhandle.environment = args.environment
    check_config(endtoend.testhandle.environment)
    set_session_headers(endtoend.testhandle.environment)


def check_config(environment_name):
    if environment_name not in endtoend.testhandle.available_environments:
        raise NameError(
            f"{datetime.utcnow()} - "
            f"Environment '{environment_name}' not found in config file {CONFIG_FILE}"
        )
    log.info("Using environment %s", environment_name)


def set_session_headers(environment_name):
    environment = endtoend.testhandle.available_environments[environment_name]

    headers = {
        "X-Auth-Token": environment.service_account.token,
        "Content-Type": "application/json",
    }
    endtoend.testhandle.session.headers.update(headers)


def get_url(path):
    environment = endtoend.testhandle.available_environments[
        endtoend.testhandle.environment
    ]

    if any(endpoint in path for endpoint in WORKFLOW_ENDPOINTS):
        url = environment.workflow_api_url + path
    elif any(endpoint in path for endpoint in XPL_ENDPOINTS):
        url = environment.xpl_api_url + path
    elif any(endpoint in path for endpoint in DATA_LOADER_ENDPOINTS):
        url = environment.data_loader_api_url + path
    elif any(endpoint in path for endpoint in PAYMENTS_HUB_ENDPOINTS):
        url = environment.payments_hub_url + path
    elif any(endpoint in path for endpoint in PROMETHEUS_ENDPOINTS):
        url = environment.prometheus_api_url + path
    else:
        url = environment.core_api_url + path
    return url


def send_request(
    method: str,
    path: str,
    data: Optional[
        Union[bytes, str, Dict[str, str], Dict[str, List[str]], List[Tuple[str, str]]]
    ] = None,
    params: Optional[
        Union[bytes, str, Dict[str, str], Dict[str, List[str]], List[Tuple[str, str]]]
    ] = None,
):
    url = get_url(path)

    r = endtoend.testhandle.session.request(method, url, params=params, data=data)
    if not r.ok:
        log.debug(
            "Method: {}, url {}, params {}, data {}".format(method, url, params, data)
        )
        # This remains as debug as there are genuine cases where expect error responses from
        # Core API and wouldn't want them to be misinterpreted as actual issues
        log.debug(r.content)

    r.raise_for_status()

    response = json.loads(r.content)
    return response


def list_resources(
    endpoint: str,
    params: Optional[Union[Dict[str, str], Dict[str, List[str]]]] = None,
    page_size: int = 50,
    result_limit: int = 0,
    api_version: str = "v1",
) -> List[Dict[str, Any]]:
    """
    Reusable method to get all or a subset of results from a paginated endpoint
    :param endpoint: the api endpoint (e.g. balances, account-schedule-updates). Combined with the
    api version to construct the API path to be added to the base url
    :param params: dictionary of parameters to add to the request url
    :param page_size: page size per request. Adjusted for the final request if a result limit
    is specified and page size > (result limit - results retrieved)
    :param result_limit: maximum number of resources to retrieve. 0 or less treated as no limit
    :param api_version: the Vault api version. Not to be confused with Vault version. Combined with
    endpoint to construct the API path the API path to be added to the base url
    :return: List of resources from the relevant endpoint
    """

    results = []
    params = params or {}
    page_token = ""

    path = f"/{api_version}/{endpoint}"

    while True:

        if result_limit > 0:
            page_size = min(page_size, result_limit - len(results))
        params.update({"page_size": str(page_size), "page_token": page_token})
        resp = send_request("get", path, params=params)
        results.extend(list(resp.values())[0])
        page_token = resp["next_page_token"]
        if page_token == "" or (0 < result_limit <= len(results)):
            break

    return results


def teardown():
    log.info("Tearing down test")
    for customer in endtoend.testhandle.customers:
        accounts_list = endtoend.core_api_helper.get_customer_accounts(customer)
        for account in accounts_list:
            endtoend.testhandle.accounts.add(account["id"])
        endtoend.core_api_helper.set_customer_status(
            customer, "CUSTOMER_STATUS_DECEASED"
        )

    endtoend.workflows_helper.delete_all_workflows()
    endtoend.contracts_helper.teardown_all_accounts()
    endtoend.supervisors_helper.close_all_plans()
    endtoend.contracts_helper.deactivate_all_calendars()


def _retry_inner(
    func: Callable,
    expected_result: Any = None,
    result_wrapper: Callable = None,
    max_retries: int = 5,
    timeout: float = 0,
    sleep_time: float = 1,
    back_off: float = 1,
    exceptions=Exception,
    return_wrapped_result: bool = False,
    failure_message: str = "Error",
) -> Any:
    """
    Retry the function if exceptions of the specified types are raised until either:
    - the optional expected result is returned
    - max_retries count is reached
    - timeout is reached
    Retries are separated by a sleep of the specified duration, increasing by the back_off factor
    :param func: Callable that returns the result
    :param expected_result: expected result to poll for. Optional
    :param result_wrapper: Callable to transform func's return.
    :param max_retries: int, number of times func will be checked
    :param timeout: float, total seconds to elapse before timing out. 0 means no timeout
    :param sleep_time: float, number of seconds to wait between calls to func
    :param back_off: float, multiplication factor for the sleep time between retries. If set to 1,
    sleep time is unchanged between each retry.
    :param exceptions: an exception of tuple of exceptions that will be caught and retried.
    :param return_wrapped_result: if true and a result_wrapper is passed in, the transformed result
    is returned. Otherwise the raw function result is returned
    :param return_wrapped_result: if true and a result_wrapper is passed in, the transformed result
    is returned. Otherwise the raw function result is returned
    :return: the function return, potentially transformed by the result_wrapper (see
    return_wrapped_result)
    """
    start_time = datetime.today()
    num_tries = 0
    _sleep_time = sleep_time

    while True:
        try:
            original_result = func()
            wrapped_result = (
                result_wrapper(original_result) if result_wrapper else original_result
            )
            # We may want expected results like {} and [] to be checked against
            if expected_result is not None and expected_result != wrapped_result:
                if result_wrapper:
                    raise ValueError(
                        f"{datetime.utcnow()} - "
                        f"Wrapped result {wrapped_result} does not match"
                        f" {expected_result}. wrapper: {result_wrapper}. Original"
                        f" result: {original_result}"
                    )
                else:
                    raise ValueError(
                        f"{datetime.utcnow()} - "
                        f"Result {wrapped_result} does not match {expected_result}"
                    )
            return wrapped_result if return_wrapped_result else original_result
        except exceptions as e:
            if num_tries >= max_retries:
                log.exception(f"{failure_message} - Retry count exceeded")
                raise e
            if timeout and datetime.today() - start_time > timedelta(seconds=timeout):
                log.exception(f"{failure_message} - Timeout reached")
                raise e
            sleep(_sleep_time)
            _sleep_time *= back_off
            num_tries += 1
            log.debug(f"Retrying {func}. Total tries: {num_tries}")


def retry_decorator(
    expected_result: Any = None,
    result_wrapper: Callable = None,
    max_retries: int = 5,
    timeout: float = 0,
    sleep_time: float = 1,
    back_off: float = 1,
    exceptions=Exception,
    return_wrapped_result: bool = False,
) -> Callable:
    """
    Creates a decorator that wraps the function with _retry_inner. See this method's doc string for
    more detail
    :param expected_result: expected result to poll for
    :param result_wrapper: callable to transform the decorated function's return. Optional
    :param max_retries: int, number of times func will be checked
    :param timeout: float, total seconds to elapse before timing out. 0 means no timeout
    :param sleep_time: float, number of seconds to wait between calls to func
    :param back_off: float, multiplication factor for the sleep time between retries. If set to 1,
    sleep time is unchanged between each retry.
    :param exceptions: an exception of tuple of exceptions that will be caught and retried.
    :param return_wrapped_result: if true and a result_wrapper is passed in, the transformed result
    is returned. Otherwise the raw function result is returned
    :return: decorator
    """

    def wrapper(func):
        @wraps(func)
        def decorator(*args, **kwargs):
            return _retry_inner(
                func=partial(func, *args, **kwargs),
                expected_result=expected_result,
                result_wrapper=result_wrapper,
                max_retries=max_retries,
                timeout=timeout,
                sleep_time=sleep_time,
                back_off=back_off,
                exceptions=exceptions,
                return_wrapped_result=return_wrapped_result,
            )

        return decorator

    return wrapper


def retry_call(
    func: Callable,
    f_args=None,
    f_kwargs=None,
    expected_result: Any = None,
    result_wrapper: Callable = None,
    max_retries: int = 5,
    timeout: float = 0,
    sleep_time: float = 1,
    back_off: float = 1,
    exceptions=Exception,
    return_wrapped_result: bool = False,
    failure_message: str = "Error",
) -> Any:
    """
    Wraps a function with _retry_inner. See this method's doc string for more detail
    :param func: function to retry
    :param f_args: the function's positional arguments
    :param f_kwargs: the function's kw args
    :param expected_result: expected result to poll for
    :param result_wrapper: callable to transform the decorated function's return. Optional
    :param max_retries: int, number of times func will be checked
    :param timeout: float, total seconds to elapse before timing out. 0 means no timeout
    :param sleep_time: float, number of seconds to wait between calls to func
    :param back_off: float, multiplication factor for the sleep time between retries. If set to 1,
    sleep time is unchanged between each retry.
    :param exceptions: an exception of tuple of exceptions that will be caught and retried.
    :param return_wrapped_result: if true and a result_wrapper is passed in, the transformed result
    is returned. Otherwise the raw function result is returned
    :param failure_message: if specified, this message will be included in any exception raised by
    the wrapper (e.g. timeout reached, max retries attempted)
    :return: the function return, potentially transformed by the result_wrapper (see
    return_wrapped_result)
    """
    f_args = f_args or []
    f_kwargs = f_kwargs or {}
    return _retry_inner(
        func=partial(func, *f_args, **f_kwargs),
        expected_result=expected_result,
        result_wrapper=result_wrapper,
        max_retries=max_retries,
        timeout=timeout,
        sleep_time=sleep_time,
        back_off=back_off,
        exceptions=exceptions,
        return_wrapped_result=return_wrapped_result,
        failure_message=failure_message,
    )


if __name__ == "__main__":
    pass
