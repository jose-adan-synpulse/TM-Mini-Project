# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
# standard libs
import argparse
import copy
import itertools
import json
import logging
import os
import math
import time
import uuid
import re
import random
from decimal import Decimal
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from json import dumps
from queue import Queue
from threading import Event, Lock, Thread
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
    DefaultDict,
    Iterator,
)
import sys

# third party
import yaml
from requests import HTTPError

# common
from common.kafka.kafka import wait_for_messages
import common.test_utils.endtoend as endtoend
from common.test_utils.performance.data_objects.data_objects import (
    AccountsPostingsProducerResults,
    AccountPostingsInProgress,
    ExpectedOutcomeValidationResults,
)
from common.test_utils.common.balance_helpers import (
    Balance,
    ExpectedBalanceComparison,
    BalanceDimensions,
)
from common.python.file_utils import load_file_contents
from common.test_utils.common.date_helper import extract_date
from common.test_utils.contracts.simulation.data_objects.data_objects import (
    ContractConfig,
)
from common.test_utils.postings.posting_classes import CustomInstruction, Posting
from common.test_utils.postings.postings_helper import create_posting_instruction_batch
from common.test_utils.contracts.simulation.simulation_test_utils import get_balances
from common.test_utils.contracts.simulation import vault_caller
from common.test_utils.contracts.simulation.helper import (
    create_account_instruction,
    create_inbound_hard_settlement_instruction,
    create_outbound_hard_settlement_instruction,
    create_flag_definition_event,
    create_flag_event,
)
from common.test_utils.endtoend import data_loader_helper, testhandle
from common.test_utils.endtoend.accounts_helper import ACCOUNT_UPDATE_EVENTS_TOPIC
from common.test_utils.endtoend.core_api_helper import init_postings_api_client
from common.test_utils.endtoend.balances import (
    ACCOUNT_BALANCE_EVENTS_TOPIC,
    create_balance_dict,
)
from common.test_utils.endtoend.data_loader_helper import (
    DATA_LOADER_EVENTS_TOPIC,
    DATA_LOADER_REQUEST_TOPIC,
    BatchResourceIds,
    wait_for_batch_events,
)
from common.test_utils.endtoend.postings import (
    MIGRATIONS_POSTINGS_RESPONSES_TOPIC,
    POSTINGS_API_RESPONSE_TOPIC,
    create_and_produce_posting_request,
)
from common.test_utils.endtoend.schedule_helper import (
    SCHEDULER_OPERATION_EVENTS_TOPIC,
    trigger_next_schedule_execution,
    wait_for_schedule_operation_events,
)
from common.test_utils.performance.prometheus_api_helper import (
    get_results,
)
from common.test_utils.performance.test_types import PerformanceTestType

from common.test_utils.endtoend.workflows_helper import (
    wait_for_wf_instantiation_messages,
    CREATE_WORKFLOW_INSTANCE_TOPIC,
)

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

PIB_OUTPUT_FIELDS = [
    "create_request_id",
    "error",
    "id",
    "insertion_timestamp",
    "status",
]

PI_OUTPUT_FIELDS = [
    "account_violations",
    "committed_postings",
    "contract_violations",
    "id",
    "posting_violations",
    "restriction_violations",
]

# These are the only instruction accepted by the postings migrator
VALID_INSTRUCTIONS = [
    "custom_instruction",
    "inbound_hard_settlement",
    "outbound_hard_settlement",
]

CONTRACT_EMPTY_FILE = (
    "common/test_utils/contracts/simulation/mock_product/empty_contract.py"
)

COMPLETED_THREAD = "completed"
INCORRECT_OUTCOME_LOG = "account_discrepancies.log"

MIGRATION_CLIENT_ID = "Migration"

# Services to extract metrics from, for each test type
DEFAULT_APPS_FOR_TEST_TYPE = {
    PerformanceTestType.POSTINGS: ["contract-engine", "vault-postings-processor"],
    PerformanceTestType.SCHEDULES: ["contract-engine"],
}

DEFAULT_MATCHED_MESSAGE_TIMEOUT = 60
DEFAULT_INTER_MESSAGE_TIMEOUT = 60


class PerformanceTest(endtoend.AcceleratedEnd2EndTest):

    product_name: str = ""
    contract_contents: Dict[str, str] = {}
    contracts: Dict[str, str] = {}
    default_tags: Dict = {}
    paused_tags: Dict = {}
    sim_contracts: Dict[str, ContractConfig] = {}
    simulation_client: vault_caller.Client
    test_profile: Dict = {}

    @classmethod
    def setUpClass(cls, product_name) -> None:
        cls.product_name = product_name
        # Ensure we can see full details of assertion failures
        cls.maxDiff = None

        # Ensure correct config for contract modules is set
        test_uses_contract_modules = False
        for sim_contract in cls.sim_contracts.values():
            if sim_contract.linked_contract_modules:
                test_uses_contract_modules = True

        if (
            endtoend.testhandle.CONTRACT_MODULES and not test_uses_contract_modules
        ) or (not endtoend.testhandle.CONTRACT_MODULES and test_uses_contract_modules):
            raise Exception(
                "The product being tested uses contract modules but no configuration was "
                "provided. Declare using ContractConfig.linked_contract_modules and endtoend."
                "testhandle.CONTRACT_MODULES."
            )

        # Load the contract for the product being tested
        cls.contracts.update(
            {product_name: endtoend.testhandle.CONTRACTS[product_name]["path"]}
        )
        cls.load_contracts()

        endtoend.helper.setup_environments()
        env_config = endtoend.testhandle.available_environments[
            endtoend.testhandle.environment
        ]
        cls.simulation_client = vault_caller.Client(
            core_api_url=env_config.core_api_url,
            auth_token=env_config.service_account.token,
        )

        topics = [
            MIGRATIONS_POSTINGS_RESPONSES_TOPIC,
            POSTINGS_API_RESPONSE_TOPIC,
            ACCOUNT_BALANCE_EVENTS_TOPIC,
            DATA_LOADER_EVENTS_TOPIC,
            DATA_LOADER_REQUEST_TOPIC,
            ACCOUNT_UPDATE_EVENTS_TOPIC,
            SCHEDULER_OPERATION_EVENTS_TOPIC,
            CREATE_WORKFLOW_INSTANCE_TOPIC,
        ]
        endtoend.kafka_setup(topics)

    @classmethod
    def load_contracts(cls) -> None:
        """
        Load the required contract files and set up the empty internal contract, for use in
        simulation. The empty internal contract, primarily for internal accounts has key "EMPTY"
        and is always loaded with id '1'. The test-specific contract setup is left to the
        individual test classes
        """

        for contract_name, path in cls.contracts.items():
            cls.contract_contents[contract_name] = str(load_file_contents(path))

        cls.contract_contents["EMPTY"] = str(load_file_contents(CONTRACT_EMPTY_FILE))

        cls.sim_contracts["empty_contract"] = ContractConfig(
            contract_file_path=CONTRACT_EMPTY_FILE,
            template_params={},
            account_configs=[],
            smart_contract_version_id="1",
        )

    @classmethod
    def tearDownClass(cls):
        # We may want to make this migrate accounts to a dummy contract, but it would potentially
        # interfere with other test runs

        for consumer in endtoend.testhandle.kafka_consumers.values():
            consumer.close()

    def setUp(self):
        # Accelerated E2E tests need per-test tags to control schedules separately, so we perform
        # the standard setup on a per-test level inside of per-test-class
        # standard_setup()
        self._started_at = time.time()

    def load_profile(self, file_path: str):
        with open(file_path, "r", encoding="utf-8") as test_profile_file:
            self.test_profile = yaml.safe_load(test_profile_file.read())

    def extract_test_instances(self) -> None:
        # Optional override of the `_profile.yaml` `instances` parameter for
        # PerformanceTestType.SCHEDULES tests
        args, _ = endtoend.extract_args()
        if args.instances:
            for dg in self.test_profile["dataloader_setup"]["dependency_groups"]:
                dg["instances"] = args.instances
            log.info(f"Updated test instances to {args.instances}")

    def extract_contract_template_parameters(self) -> None:
        contract_name = self.test_profile["dataloader_setup"]["contract_name"]
        if contract_name not in testhandle.CONTRACTS:
            raise Exception(
                f"The contract in the test profile ({contract_name}) could "
                "not be found. Please ensure the correct contract is loaded."
            )

        template_params = self.test_profile["dataloader_setup"].get(
            "template_param_vals"
        )
        if template_params:
            for param in template_params:
                if isinstance(template_params, dict):
                    template_params[param] = dumps(template_params[param])
            testhandle.CONTRACTS[contract_name]["template_params"].update(
                template_params
            )

    def setup_performance_test_profile(self) -> List[str]:
        self._check_environment_sufficient()
        for dg in self.test_profile["dataloader_setup"]["dependency_groups"]:
            id_base = dg["customer"]["id_base"]
            if id_base < 100 or id_base > 999:
                log.warn(f"Unexpected id_base: {id_base}, should be between 100 - 999")
        batched_ids = create_dataloader_resources(
            endtoend.testhandle.kafka_producer, self.test_profile["dataloader_setup"]
        )
        all_accounts_to_process = [
            account_id
            for batch_resource_ids in batched_ids.values()
            for account_id in batch_resource_ids.account_ids
        ]

        # Generate posting template using a full day's simulation
        postings_template = generate_postings_template(
            self.product_name,
            simulation_client=self.simulation_client,
            simulation_contracts=self.sim_contracts,
            simulation_setup=self.test_profile.get("simulation_setup"),
        )

        init_postings_api_client(
            client_id=MIGRATION_CLIENT_ID,
            response_topic=MIGRATIONS_POSTINGS_RESPONSES_TOPIC,
        )

        postings_producer = AccountsPostingsProducer(
            account_ids=all_accounts_to_process,
            postings_template=postings_template,
        )
        postings_producer_thread = Thread(target=postings_producer.create_postings)
        postings_producer_thread.start()

        batch_handler = BatchUpdatedHandler(
            batched_ids=batched_ids,
        )

        wait_for_batch_events(set(batched_ids.keys()), batch_handler)
        log.info("Finished waiting for batch events.")

        if not batch_handler.loaded_accounts:
            raise Exception("No accounts were loaded successfully")

        log.info("Wait for postings producer thread ...")
        postings_producer_thread.join()
        log.info("Finished waiting for postings producer thread.")

        return postings_producer.results.accounts_loaded

    def validate_expected_outcome(
        self,
        account_ids: List[str],
        matched_message_timeout: int = DEFAULT_MATCHED_MESSAGE_TIMEOUT,
        inter_message_timeout: int = DEFAULT_INTER_MESSAGE_TIMEOUT,
    ) -> Tuple[bool, str]:
        expected_outcome = self.test_profile.get("expected_outcome")
        expected_balances_list = expected_outcome.get("balances")
        expected_workflows_list = expected_outcome.get("workflows")

        number_of_accounts = len(account_ids)

        if expected_outcome is None:
            log.info("No expected outcomes have been configured.")
            return True, "No validations configured"

        if expected_balances_list is None:
            log.info("No expected balances have been configured.")

        if expected_workflows_list is None:
            log.info("No expected workflows have been configured.")

        expected_balances = get_expected_balances(expected_balances_list)
        expected_workflows = _map_expected_workflow_ids_to_e2e_ids(
            expected_workflows_list
        )

        expected_outcome_consumers = ExpectedOutcomeConsumer(
            account_ids, expected_balances, expected_workflows
        )

        expected_outcome_consumers.listen_to_consumers(
            matched_message_timeout=matched_message_timeout,
            inter_message_timeout=inter_message_timeout,
        )

        account_ids_with_incorrect_balances = (
            expected_outcome_consumers.results.accounts_with_incorrect_balances
        )
        account_ids_with_missing_workflows = (
            expected_outcome_consumers.results.accounts_with_missing_workflows
        )

        if (
            len(account_ids_with_incorrect_balances) == 0
            and len(account_ids_with_missing_workflows) == 0
        ):
            return True, "No issues detected"

        account_logger = logging.getLogger("account_logger")
        file_handler = logging.FileHandler(INCORRECT_OUTCOME_LOG)
        account_logger.addHandler(file_handler)
        account_logger.info(
            f"Percentage of accounts with incorrect balances: "
            f"{len(account_ids_with_incorrect_balances)*100/number_of_accounts}%"
        )
        account_logger.info(
            f"Accounts with incorrect balances: {account_ids_with_incorrect_balances}"
        )

        account_logger.info(
            f"Percentage of accounts with missing workflows: "
            f"{len(account_ids_with_missing_workflows)*100/number_of_accounts}%"
        )
        account_logger.info(
            f"Accounts with differences between expected and seen workflows: "
            f"{account_ids_with_missing_workflows}"
        )

        account_logger.removeHandler(file_handler)

        message = "Discrepancies found in expected outcome: "
        if len(account_ids_with_incorrect_balances) != 0:
            message += (
                f"Invalid balances detected in "
                f"{len(account_ids_with_incorrect_balances)*100/number_of_accounts}% of accounts. "
            )

        if len(account_ids_with_missing_workflows) != 0:
            message += (
                f"Missing workflows in "
                f"{len(account_ids_with_missing_workflows)*100/number_of_accounts}% "
                f"of accounts. "
            )

        return False, message + f"See {os.getcwd()} for problem accounts."

    def process_args(self, args: list) -> argparse.Namespace:
        """
        Process command-line arguments to the tool
        """
        parser = argparse.ArgumentParser(
            description="Performance helper provides a "
            "framework for running performance tests",
        )

        # Common arguments
        parser.add_argument(
            "--create_report",
            type=bool,
            default=False,
            action="store",
            help="Whether to create automated report files using the prometheus API helper",
        )
        known_args, _ = parser.parse_known_args(args)
        return known_args

    def run_performance_test(
        self,
        test_profile_filepath: str,
        test_type: PerformanceTestType = PerformanceTestType.SCHEDULES,
    ) -> None:
        self.load_profile(test_profile_filepath)
        args = self.process_args(sys.argv[1:])

        self.extract_contract_template_parameters()
        endtoend.standard_setup()
        if test_type == PerformanceTestType.SCHEDULES:
            self.extract_test_instances()
            account_ids = self.setup_performance_test_profile()
            start_time = datetime.now(tz=timezone.utc)
            self._schedules_performance_test(account_ids)
        elif test_type == PerformanceTestType.POSTINGS:
            # This function sets the number of dependency group instances
            # So must be run before dataloader setup
            stages = self._configure_postings_stages()
            account_ids = self.setup_performance_test_profile()
            start_time = datetime.now(tz=timezone.utc)
            self._postings_performance_test(stages, account_ids)
        else:
            valid_types = [t.name for t in PerformanceTestType]
            raise Exception(
                f"Unable to determine PerformanceTestType, expected one of {valid_types}"
                f", got {test_type}"
            )
        if args.create_report:
            get_results(
                apps=DEFAULT_APPS_FOR_TEST_TYPE[test_type],
                start=start_time,
                end=datetime.now(tz=timezone.utc),
                environment=endtoend.testhandle.environment,
                test_type=test_type,
            )

    def _schedules_performance_test(self, account_ids: List[str]) -> None:
        first = True
        for tag, details in self.paused_tags.items():
            log.info(f"Triggering next schedule execution: {tag}")
            if (
                not first
                and hasattr(endtoend.testhandle, "paused_schedule_tag_delay")
                and endtoend.testhandle.paused_schedule_tag_delay > 0
            ):
                log.info(
                    "Entering delay before triggering next schedule execution: "
                    + str(endtoend.testhandle.paused_schedule_tag_delay)
                    + " seconds"
                )
                time.sleep(endtoend.testhandle.paused_schedule_tag_delay)
            first = False
            trigger_next_schedule_execution(
                paused_tag_id=tag, schedule_frequency=details["schedule_frequency"]
            )
            # Wait up to 10 mins for schedules to complete
            wait_for_schedule_operation_events(
                tag_names=[tag],
                inter_message_timeout=600,
                matched_message_timeout=600,
            )

        correct_output, detail = self.validate_expected_outcome(account_ids)
        self.assertTrue(correct_output, msg=detail)

    def _configure_postings_stages(self) -> Dict[str, int]:
        # Stage range will satisfy most cases, but individual stages can be specified in the
        # profile if required
        if "stage_range" in self.test_profile["postings_setup"]:
            stage_range = self.test_profile["postings_setup"]["stage_range"]
            duration = stage_range["duration"]
            timeout = stage_range["timeout"]
            postings_per_acc = len(self.test_profile["postings_setup"]["pib_template"])
            stages = [
                {
                    "tps": tps,
                    "duration": duration,
                    "timeout": timeout,
                    "accounts_required": math.ceil(duration * tps / postings_per_acc),
                }
                for tps in range(
                    stage_range["start"], stage_range["stop"], stage_range["step"]
                )
            ]
        else:
            stages = self.test_profile["postings_setup"]["stages"]

        # Calculate the number of accounts we need
        num_accounts = max(stage["accounts_required"] for stage in stages)
        # Reduce the total number of instances to be ~= num_accounts
        dataloader_setup = self.test_profile["dataloader_setup"]
        num_dgs = len(dataloader_setup["dependency_groups"])
        for dg in dataloader_setup["dependency_groups"]:
            dg["instances"] = math.ceil(num_accounts / num_dgs)
        return stages

    def _postings_performance_test(
        self, stages: Dict[str, int], account_ids: List[str]
    ) -> None:
        pib_template = self.test_profile["postings_setup"]["pib_template"]
        populate_pib_timestamps(pib_template)
        for stage in stages:
            log.info(f"Running tps stage with stage: {stage}")
            accounts_required = stage["accounts_required"]
            postings = generate_postings_for_accounts(
                account_ids[:accounts_required],
                pib_template,
            )

            start_time = time.time()
            current_time = start_time
            posting_create_request_ids = []
            timeout = stage["timeout"]
            # processing time makes tps below desired number, so decrease sleep time a little
            sleep_time = 1 / (stage["tps"] + 3)

            for pib in postings:
                if current_time - start_time < timeout:
                    current_time = time.time()
                    posting_create_request_ids.append(
                        create_and_produce_posting_request(
                            endtoend.testhandle.kafka_producer, pib, migration=False
                        )
                    )
                    time.sleep(sleep_time)
                else:
                    log.info(
                        f"Exceeded stage timeout {timeout} "
                        f"after producing {len(posting_create_request_ids)} postings, exiting"
                    )
                    break
            endtoend.testhandle.kafka_producer.flush()

            # TODO(INC-4569): we may consider halting the test if responses are not received soon
            # enough as this suggests that the platform is not keeping up with requests and there
            # is no need to move to the next stage.

        log.info("All tps stages completed")

    def _check_environment_sufficient(self):
        is_performance_testing_environment = endtoend.testhandle.available_environments[
            endtoend.testhandle.environment
        ].performance_testing
        total_instances = sum(
            dg["instances"]
            for dg in self.test_profile["dataloader_setup"]["dependency_groups"]
        )

        if not is_performance_testing_environment and total_instances > 10:
            raise Exception(
                f"Running with {total_instances} instances. Tests with more than 10 instances "
                "should only be run on performance testing environments"
            )


class BatchUpdatedHandler(object):
    """
    Handles data loader batch updated events by:
    - recording event statuses
    - producing any required postings to the relevant accounts
    Uses REST API to handle any missing events
    """

    # List of posting create request ids
    posting_requests_sent: List[str]
    # Dict of batch ids to resource type to resource ids
    batched_ids: Dict[str, BatchResourceIds]
    # Dict of unsuccessful batch ids to corresponding status. This may not be a data-loader api
    # status if we tried to recover from the batch
    unsuccessful_batches: Dict[str, str]
    # List of batch ids for which a batch event with COMPLETE status was received
    successful_batches: List[str]
    # List of loaded ids by resource type. This will differ from the batched ids for successful
    # batches as we may have recovered successfully loaded resources from an unsuccessful batch
    loaded_accounts: List[str]
    loaded_customers: List[str]
    loaded_flags: List[str]

    def __init__(
        self,
        batched_ids: Dict[str, BatchResourceIds],
    ) -> None:
        """
        :param batched_ids: dict of batch id to BatchResourceIds
        :param postings_template: list of PIBs to instruct for each account
        """
        self.batched_ids = batched_ids
        self.loaded_accounts = []
        self.loaded_customers = []
        self.loaded_flags = []
        self.posting_requests_sent = []
        self.unsuccessful_batches = {}
        self.successful_batches = []

    def _handle_batch(self, batch: Dict, full_batch: bool = False):
        """
        Processes a dataloader resource batch. If the batch has come via kafka full_batch should be
        set to False as it will be missing info we rely on
        """
        batch_id = batch["id"]
        status = batch["status"]
        if status == "RESOURCE_BATCH_STATUS_COMPLETE":
            self._handle_successful_batch(batch_id)
        else:
            log.warning(f"Got status {status} for batch_id {batch_id}")
            self._handle_unsuccessful_batch(batch_id, batch if full_batch else None)

    def _handle_successful_batch(self, batch_id: str):
        """
        Generates and produces postings. To be used against a successful dataloader resource batch
        """
        self.successful_batches.append(batch_id)

        self.loaded_accounts.extend(self.batched_ids[batch_id].account_ids)
        self.loaded_customers.extend(self.batched_ids[batch_id].customer_ids)
        self.loaded_flags.extend(self.batched_ids[batch_id].flag_ids)

    def _handle_unsuccessful_batch(self, batch_id: str, batch: Optional[Dict] = None):
        """
        Extracts successfully loaded resources from a failed batch
        """
        if not batch:
            # We have to fetch the batch again as the kafka event doesn't currently include the
            # resource details
            try:
                batch = data_loader_helper.batch_get_resource_batches([batch_id])[
                    batch_id
                ]
            except HTTPError:
                # In is case we return now and the batch will be treated as missing later as it
                # is not in successful_batches or unsuccessful_batches
                log.warning(
                    "Failed to retrieve unsuccessful batch via REST API. This will be treated as"
                    "a missing batch in subsequent processing."
                )
                return

        loaded_accounts = []

        # itertools requires us to sort the resources before grouping
        sorted_resources = sorted(
            batch["resources"], key=lambda x: x["dependency_group_id"]
        )
        for _, group in itertools.groupby(
            sorted_resources, key=lambda x: x["dependency_group_id"]
        ):
            # group is an iterator that we process twice, so we store the contents in a list
            resources = [resource for resource in group]
            # We only want to treat a group as successful if all of its resources were loaded
            if any(
                resource
                for resource in resources
                if resource["resource_status"] != "RESOURCE_STATUS_LOADED"
            ):
                continue

            for resource in resources:
                if "account_resource" in resource:
                    self.loaded_accounts.append(resource["id"])
                    loaded_accounts.append(resource["id"])
                elif "customer_resource" in resource:
                    self.loaded_customers.append(resource["id"])
                elif "flag_resource" in resource:
                    self.loaded_flags.append(resource["id"])

        if loaded_accounts:
            self.unsuccessful_batches[batch_id] = "PARTIALLY_RECOVERED"
        else:
            self.unsuccessful_batches[batch_id] = batch["status"]

    def __call__(self, event_msg) -> Any:
        """
        When a dataloader batch is completed we trigger creation of postings to the accounts in
        the batch
        """
        self._handle_batch(event_msg["resource_batch_updated"]["resource_batch"])

    # TODO: we may consider removing this logic altogether as it originally catered for teething
    # issues with the data-loader streaming api
    def handle_missing_events(self) -> None:
        """
        Attempts to retrieve batch status via REST API for any batches that events were not
        received. Any completed batches retrieved this way are processed normally
        """
        # We must avoid reprocessing unsuccessful batches we've already recovered
        recovered_batches = {
            batch_id
            for batch_id, status in self.unsuccessful_batches.items()
            if status == "PARTIALLY_RECOVERED"
        }
        missing_ids = set(self.batched_ids.keys()).difference(
            set(self.successful_batches).union(recovered_batches)
        )
        if not missing_ids:
            return

        # We assume that all batches requests were received at this point and the API will return
        # an entry so we give up entirely if batchGet fails
        try:
            log.info("Attempting to retrieve statuses for batches with missing events")
            log.info(missing_ids)
            # TODO: we could exceed the max url length if there are too many missing batches
            batches = data_loader_helper.batch_get_resource_batches(ids=missing_ids)
            for batch in batches.values():
                self._handle_batch(batch, full_batch=True)

        except HTTPError:
            log.warning(
                "Failed to retrieve statuses for batches with missing events. Skipping them"
                "in subsequent processing"
            )
            self.unsuccessful_batches.update(
                {batch_id: "MISSING_BATCH_EVENT" for batch_id in missing_ids}
            )

        if self.unsuccessful_batches:
            log.warning(
                f"Following DL batches did not complete successfully:"
                f"{self.unsuccessful_batches}"
            )


class AccountsPostingsProducer:
    """
    Handles the loading of postings for each account using the postings template
    """

    # Event to signal an account is complete
    account_complete: Event
    # Event to signal accounts have been added to "accounts to process" queue
    accounts_added: Event
    # Track accounts in progress, storing number of postings completed and last updated for each
    accounts_in_progress: Dict[str, AccountPostingsInProgress]
    # After accounts are active, they are in "accounts to process" queue
    # until they are added to "accounts in progress"
    accounts_to_process: Deque[str]
    # Accounts waiting for activation
    accounts_waiting_activation: Deque[str]
    # Queue for accounts that are waiting for their next posting to be sent
    idle_accounts: Queue
    # Lock for thread operations on shared objects
    lock: Lock
    # Maximum concurrent accounts to process in parallel
    # If this is set too high, account postings are more likely to timeout
    max_buffer_size: int = 1000
    # Maximum time to wait in seconds between sending a posting request and receiving a balance
    # update for an account before reporting an error.
    message_timeout: int = 600
    # The posting requests currently in progress: posting request ID -> account ID
    postings_requests_in_progress: Dict[str, str]
    # Simulation test generated postings template used for all account postings
    postings_template: List
    # Event to signal idle accounts are ready to be processed
    process_idle_accounts_queue: Event
    # Results object
    results: AccountsPostingsProducerResults
    # Event to signal that any working threads can now close
    processing_complete: Event
    total_added_to_in_progress: int = 0
    total_accounts_to_process: int = 0

    def __init__(
        self,
        account_ids: List[str],
        postings_template: List,
    ) -> None:
        self.account_complete = Event()
        self.accounts_added = Event()
        self.accounts_in_progress = {}
        self.accounts_to_process = deque()
        self.accounts_waiting_activation = deque(account_ids)
        self.idle_accounts = Queue()
        self.lock = Lock()
        self.number_of_postings = len(postings_template)
        self.postings_requests_in_progress = {}
        self.postings_template = postings_template
        self.process_idle_accounts_queue = Event()
        self.processing_complete = Event()
        self.results = AccountsPostingsProducerResults()
        self.total_accounts_to_process = len(self.accounts_waiting_activation)
        self.total_added_to_in_progress = 0

    def create_postings(self) -> None:
        """
        Produces PIB requests over Kafka using the postings template for each account in
        accounts_to_process.
        """
        log.info(
            f"Producing {len(self.postings_template)} posting requests for "
            f"{self.total_accounts_to_process} accounts."
        )

        with ThreadPoolExecutor(max_workers=6) as executor:
            future_to_thread_name = {
                executor.submit(
                    self._account_activations_consumer
                ): "account_activations_consumer",
                executor.submit(
                    self._fill_accounts_in_progress
                ): "fill_accounts_in_progress",
                executor.submit(self._postings_producer): "postings_producer",
                executor.submit(
                    self._postings_responses_consumer
                ): "postings_responses_consumer",
                executor.submit(
                    self._balance_updates_consumer
                ): "balance_updates_consumer",
                executor.submit(
                    self._postings_timeout_auditor
                ): "postings_timeout_auditor",
            }
            for future in as_completed(future_to_thread_name.keys()):
                thread_name = future_to_thread_name[future]
                try:
                    future_result = future.result()
                except Exception as exc:
                    log.warn(f"Thread {thread_name} raised exception: {exc}")
                else:
                    if future_result == COMPLETED_THREAD:
                        log.info(f"Thread {thread_name} completed.")
                    else:
                        log.warn(
                            f"Thread {thread_name} completed with unexpected "
                            f"return value {future_result}."
                        )

        if self.results.accounts_failed:
            log.warning(
                f"{len(self.results.accounts_failed)} accounts failed to load postings"
            )

        log.info("finished create postings")

    def _account_activations_consumer(self) -> str:
        """
        Wait for the activation of all accounts before putting them in the accounts to process
        queue. This will guarantee that postings are committed and that schedules are created.
        """
        last_message_time = time.time()
        consumer = get_account_update_events_consumer()
        while self.accounts_waiting_activation:
            msg = consumer.poll(1)
            if not msg:
                delay = time.time() - last_message_time
                if delay > self.message_timeout:
                    log.warning(
                        f"Waited {delay:.1f}s since last account activation received. "
                        f"Timeout set to {self.message_timeout:.1f}. Exiting "
                        f"after {len(self.accounts_waiting_activation)} "
                        f"accounts unactivated. The unactivated account ids were: "
                        f"{self.accounts_waiting_activation}"
                    )
                    break
                continue
            if msg.error():
                log.warn(f"account activations consumer got error: {msg.error().str()}")
                continue
            try:
                event_msg = json.loads(msg.value().decode())
            except Exception as e:
                log.warn(f"account activations consumer failed to decode message: {e}.")
                continue
            account_update_wrapper = event_msg.get("account_update_updated")
            if account_update_wrapper:
                last_message_time = time.time()
                account_update = account_update_wrapper["account_update"]
                if account_update["account_id"] in self.accounts_waiting_activation:
                    if account_update["status"] == "ACCOUNT_UPDATE_STATUS_COMPLETED":
                        self.accounts_waiting_activation.remove(
                            account_update["account_id"]
                        )
                        self.accounts_to_process.append(account_update["account_id"])
                        self.accounts_added.set()
                    elif account_update["status"] in [
                        "ACCOUNT_UPDATE_STATUS_ERRORED",
                        "ACCOUNT_UPDATE_STATUS_REJECTED",
                    ]:
                        self.accounts_waiting_activation.remove(
                            account_update["account_id"]
                        )
                        self.results.accounts_failed.append(
                            account_update["account_id"]
                        )
                        log.warn(
                            f"Failed account activation for account {account_update['account_id']}"
                        )
        return COMPLETED_THREAD

    def _fill_accounts_in_progress(self) -> str:
        """
        Add accounts from accounts_to_process to accounts_in_progress up to the max buffer size.
        This ensures we are only producing postings and waiting for their balance updates for a
        maximum number of accounts at the same time
        """
        while self.accounts_waiting_activation or self.accounts_to_process:
            if not self.accounts_added.is_set():
                self.accounts_added.wait()
                self.accounts_added.clear()
            while self.accounts_to_process:
                with self.lock:
                    while len(self.accounts_in_progress) < self.max_buffer_size:
                        if not self.accounts_to_process:
                            break
                        next_account_id = self.accounts_to_process.popleft()
                        self.accounts_in_progress[
                            next_account_id
                        ] = AccountPostingsInProgress(last_updated=datetime.now())
                        self.total_added_to_in_progress += 1
                        self.idle_accounts.put(next_account_id)
                        self.process_idle_accounts_queue.set()
                if self.accounts_to_process:
                    self.account_complete.wait()
                    self.account_complete.clear()
        return COMPLETED_THREAD

    def _postings_producer(self) -> str:
        """
        Produce posting requests for accounts in idle_accounts queue.
        For each account:
         - Produce posting request
         - Wait for _postings_responses_consumer to get the PIB ID
         - Wait for _balance_updates_consumer to get the balance event
         - Produce the next posting request until all completed
        """
        idle_account_id: str
        while (
            self.accounts_in_progress
            or self.accounts_to_process
            or self.accounts_waiting_activation
        ):
            while not self.idle_accounts.empty():
                idle_account_id = self.idle_accounts.get()
                with self.lock:
                    template_index = self.accounts_in_progress[
                        idle_account_id
                    ].postings_completed
                    next_posting = update_target_account(
                        idle_account_id, self.postings_template[template_index]
                    )
                    request_id = create_and_produce_posting_request(
                        endtoend.testhandle.kafka_producer,
                        pib=next_posting,
                        key=idle_account_id,
                        migration=True,
                    )
                    self.accounts_in_progress[idle_account_id].postings_sent += 1
                    log.debug(
                        f"PRODUCE acc {idle_account_id}, postings_sent "
                        f"{self.accounts_in_progress[idle_account_id].postings_sent}, "
                        f"postings_completed {template_index}, request_id {request_id}, "
                        f"posting {next_posting}"
                    )
                    self.accounts_in_progress[
                        idle_account_id
                    ].posting_request_id = request_id
                    self.accounts_in_progress[
                        idle_account_id
                    ].last_updated = datetime.now()
                    self.postings_requests_in_progress[request_id] = idle_account_id
                    # We need to get the PIB ID from the postings responses consumer, reset it here
                    self.accounts_in_progress[
                        idle_account_id
                    ].pib_status_in_progress = "pib_produced"
                    self.accounts_in_progress[idle_account_id].pib_pib_in_progress = ""
            self.process_idle_accounts_queue.wait()
            self.process_idle_accounts_queue.clear()
        return COMPLETED_THREAD

    def _postings_responses_consumer(self) -> str:
        """
        Consume postings responses for the postings that have been produced, getting the PIB ID
        """
        consumer = get_postings_responses_consumer()
        while (
            self.accounts_in_progress
            or self.accounts_to_process
            or self.accounts_waiting_activation
        ):
            msg = consumer.poll(1)
            if not msg:
                continue
            if msg.error():
                log.warn(f"postings responses consumer got error: {msg.error().str()}")
                continue
            try:
                event_msg = json.loads(msg.value().decode())
            except Exception as e:
                log.warn(f"postings responses consumer failed to load a posting: {e}.")
                continue
            event_id = event_msg.get("create_request_id")
            event_status = event_msg.get("status")
            pib_id = event_msg.get("id")
            if event_id in self.postings_requests_in_progress:
                account_id = self.postings_requests_in_progress[event_id]
                if event_status == "POSTING_INSTRUCTION_BATCH_STATUS_ACCEPTED":
                    self.accounts_in_progress[account_id].pib_id_in_progress = pib_id
                    self.accounts_in_progress[
                        account_id
                    ].pib_status_in_progress = "pib_consumed"
                else:
                    error = event_msg.get("error")
                    log.info(
                        "postings responses consumer pib NOT accepted: "
                        f"account_id {account_id}, event_id {event_id}, "
                        f"pib_id {pib_id}, status {event_status}, error {error}"
                    )
                    self.results.accounts_failed.append(account_id)
                    with self.lock:
                        del self.accounts_in_progress[account_id]
                        self.account_complete.set()
                del self.postings_requests_in_progress[event_id]
        return COMPLETED_THREAD

    def _balance_updates_consumer(self) -> str:
        """
        Consume balance updates
        For each account in accounts_in_progress:
         - Check the PIB ID matches to the posting that has been produced
         - When balance is received, set event so producer knows to request the next posting
        """
        consumer = get_balance_updates_consumer()
        while (
            self.accounts_in_progress
            or self.accounts_to_process
            or self.accounts_waiting_activation
        ):
            msg = consumer.poll(1)
            if not msg:
                continue
            if msg.error():
                log.warn(f"balance updates consumer got error: {msg.error().str()}")
                continue
            try:
                event_msg = json.loads(msg.value().decode())
            except Exception as e:
                log.warn(
                    f"balance updates consumer failed to load a balance update: {e}."
                )
                continue
            account_id = event_msg.get("account_id")
            pib_id = event_msg.get("posting_instruction_batch_id")
            with self.lock:
                if account_id and account_id in self.accounts_in_progress:

                    # It's possible that a balance update can be received before the postings
                    # responses consumer receives the corresponding message, so give the
                    # responses consumer a chance to catch up.
                    attempts = 0
                    while (
                        self.accounts_in_progress[account_id].pib_status_in_progress
                        == "pib_produced"
                        and attempts < 20
                    ):
                        attempts += 1
                        time.sleep(0.5)

                    pib_id_in_progress = self.accounts_in_progress[
                        account_id
                    ].pib_id_in_progress
                    pib_status_in_progress = self.accounts_in_progress[
                        account_id
                    ].pib_status_in_progress

                    if pib_status_in_progress == "pib_produced":
                        log.warn(
                            f"balance update event for account {account_id} has happened "
                            "before the postings responses consumer has received response "
                            "for the pib"
                        )
                        continue
                    elif pib_status_in_progress == "balance_updated":
                        log.info(
                            f"balance update event for account {account_id} has happened "
                            "even though we have already processed the balance update, "
                            "so this is likely to be a duplicate posting due to idempotency"
                        )
                        continue
                    elif not pib_status_in_progress:
                        log.debug(
                            f"balance update event for account {account_id} has happened "
                            "before a posting has been produced, which may be due to a posting"
                            "triggered from post activate code hook"
                        )
                        continue
                    elif pib_id_in_progress != pib_id:
                        log.debug(
                            "balance update consumer skipped a non matching event: "
                            f"pib_id_in_progress '{pib_id_in_progress}', pib_id '{pib_id}'"
                            f"pib_status_in_progress '{pib_status_in_progress}', "
                            f"event_msg {event_msg}. "
                            "This may be due to a posting triggered from post activate code hook"
                        )
                        continue

                    # this balance update corresponds to pib ID in progress
                    self._complete_a_posting(account_id)

        # When we get to the end of accounts queue, accounts_in_progress is being emptied
        # with nothing left to add in. At this point, in _postings_producer, there is a wait for
        # process_idle_accounts_queue event which still needs to be cleared.
        self.process_idle_accounts_queue.set()
        self.processing_complete.set()
        return COMPLETED_THREAD

    def _postings_timeout_auditor(self) -> str:
        """
        Check if any currently executing postings have timed out or failed,
        and periodically log a progress update.
        """
        while (
            self.accounts_in_progress
            or self.accounts_to_process
            or self.accounts_waiting_activation
        ):
            with self.lock:
                log.info(
                    "postings timeout auditor ..."
                    f"\n  total accounts to process: {self.total_accounts_to_process}"
                    f"\n  waiting for activation: {len(self.accounts_waiting_activation)}"
                    f"\n  total added to in progress: {self.total_added_to_in_progress}"
                    f"\n  loaded successfully: {len(self.results.accounts_loaded)}"
                    f"\n  accounts failed: {len(self.results.accounts_failed)}"
                    f"\n  idle accounts: {self.idle_accounts.qsize()}"
                    f"\n  accounts in progress: {len(self.accounts_in_progress)}"
                    f"\n  postings in progress: {len(self.postings_requests_in_progress)}"
                )
                # Iterate through a copy of keys in accounts_in_progress
                # so that we can delete or add to the original dict in the same iteration
                for account_id in list(self.accounts_in_progress.keys()):
                    if (
                        datetime.now()
                        - self.accounts_in_progress[account_id].last_updated
                    ).total_seconds() > self.message_timeout:
                        if (
                            self.accounts_in_progress[account_id].pib_status_in_progress
                            == "pib_consumed"
                        ):
                            # When migrating postings on active accounts, as we do in performance
                            # tests, it is possible for race condition to occur between the first
                            # posting produced here and the one from the account activation code
                            # hook, resulting in the balance update event to get missed
                            log.warn(
                                "PIB accepted but timed out waiting for balance update event. "
                                "Ignore this and continue producing postings. "
                                f"account: {account_id}, "
                                f"tracker: {self.accounts_in_progress[account_id]}"
                            )
                            self._complete_a_posting(account_id)
                        else:
                            log.warn(
                                f"account_id {account_id} timed out waiting for balances, "
                                f"tracker: {self.accounts_in_progress[account_id]}"
                            )
                            del self.accounts_in_progress[account_id]
                            self.results.accounts_failed.append(account_id)
                            self.account_complete.set()

            # We need the timeout auditor to run periodically, every 60 seconds,
            # but it should terminate as soon as processing is complete
            self.processing_complete.wait(60)
        return COMPLETED_THREAD

    def _complete_a_posting(self, account_id: str) -> None:
        self.accounts_in_progress[account_id].pib_status_in_progress = "balance_updated"
        self.accounts_in_progress[account_id].postings_completed += 1
        postings_completed = self.accounts_in_progress[account_id].postings_completed
        postings_sent = self.accounts_in_progress[account_id].postings_sent
        if postings_completed >= self.number_of_postings:
            log.debug(
                f"CONSUME Complete acc {account_id}, postings_sent {postings_sent},"
                f" postings_completed {postings_completed}"
            )
            del self.accounts_in_progress[account_id]
            self.results.accounts_loaded.append(account_id)
            self.account_complete.set()
        else:
            log.debug(
                f"CONSUME acc {account_id}, postings_sent {postings_sent}, "
                f"postings_completed {postings_completed}"
            )
            self.idle_accounts.put(account_id)
            self.process_idle_accounts_queue.set()


class ExpectedOutcomeConsumer:

    results = ExpectedOutcomeValidationResults()

    def __init__(
        self,
        account_ids: List[str],
        expected_balances: Dict[BalanceDimensions, ExpectedBalanceComparison],
        expected_workflows: Dict[str, int],
    ) -> None:
        self.account_ids = account_ids
        self.expected_balances = (
            {acc_id: copy.deepcopy(expected_balances) for acc_id in account_ids}
            if expected_balances
            else {}
        )
        self.expected_workflows = expected_workflows if expected_workflows else {}

    def listen_to_consumers(
        self, matched_message_timeout: int = 30, inter_message_timeout: int = 30
    ) -> None:
        log.info("Validating expected outcome")
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(
                    self._wait_for_account_balances_by_ids,
                    self.expected_balances,
                    matched_message_timeout,
                    inter_message_timeout,
                ): "wait_for_account_balances_by_id",
                executor.submit(
                    self._wait_for_wf_instantiation_messages,
                    self.account_ids,
                    self.expected_workflows,
                    matched_message_timeout,
                    inter_message_timeout,
                ): "wait_for_wf_instantiation_messages",
            }

            for future in as_completed(futures):
                thread_name = futures[future]
                try:
                    future_result = future.result()
                except Exception as exc:
                    log.warn(f"Thread {thread_name} raised exception: {exc}")
                else:
                    if future_result == COMPLETED_THREAD:
                        log.info(f"Thread {thread_name} completed.")
                    else:
                        log.warn(
                            f"Thread {thread_name} completed with unexpected "
                            f"return value {future_result}."
                        )

    def _wait_for_account_balances_by_ids(
        self,
        expected_balances: Dict[
            str, Dict[BalanceDimensions, ExpectedBalanceComparison]
        ],
        matched_message_timeout,
        inter_message_timeout,
    ):
        consumer = endtoend.testhandle.kafka_consumers[ACCOUNT_BALANCE_EVENTS_TOPIC]

        def matcher(event_msg, unique_message_ids):
            event_account_id = event_msg["account_id"]
            event_request_id = event_msg["event_id"]
            if event_account_id in unique_message_ids:
                balance_wrapper = event_msg.get("balances")
                actual_balances = create_balance_dict(balance_wrapper)
                for dimension, expected_balance in expected_balances[
                    event_account_id
                ].items():
                    if dimension in actual_balances and (
                        expected_balance.actual_balance is None
                        or actual_balances[dimension].value_timestamp
                        >= expected_balance.actual_balance.value_timestamp
                    ):
                        expected_balance.set_actual_balance(actual_balances[dimension])
                return ("", event_request_id, True)
            return ("", event_request_id, False)

        failed_account_ids = wait_for_messages(
            consumer,
            matcher=matcher,
            callback=None,
            unique_message_ids=expected_balances,
            matched_message_timeout=matched_message_timeout,
            inter_message_timeout=inter_message_timeout,
        )

        formatted_failed_account_ids = defaultdict(dict)
        for acc_id, details in failed_account_ids.items():
            for dimension, expected_balance_object in details.items():
                if expected_balance_object.has_difference():
                    formatted_failed_account_ids[acc_id][dimension] = (
                        expected_balance_object.get_expected_and_actual_as_dict()
                        or "No balance updates seen"
                    )

        self.results.accounts_with_incorrect_balances = formatted_failed_account_ids
        return COMPLETED_THREAD

    def _wait_for_wf_instantiation_messages(
        self,
        account_ids: List[str],
        expected_workflows: Dict[str, int],
        matched_message_timeout,
        inter_message_timeout,
    ):
        account_ids = account_ids if expected_workflows else []
        self.results.accounts_with_missing_workflows = (
            wait_for_wf_instantiation_messages(
                account_ids=account_ids,
                expected_wf=expected_workflows,
                matched_message_timeout=matched_message_timeout,
                inter_message_timeout=inter_message_timeout,
            )
        )
        return COMPLETED_THREAD


def extract_postings_template(
    res: List[Dict],
    simulation_setup: Dict,
    tside: str = "LIABILITY",
    migrate_balance_ts: bool = True,
) -> List[Dict]:
    """
    Given the outputs of a simulation, extract the posting instruction batches
    and remove the output fields
    :param res: the outputs of the simulation
    :param tside: the account tside. One of 'ASSET' or 'LIABILITY'. Used to determine
    :param migrate_balance_ts: indicates whether we want to the balance timeseries (True) or just
    the final values (False). If False, a single posting instruction per final balance is added to
    the template, instead of every single posting.
    :return: a list of posting instruction batches to set up the accounts as per simulation outcome
    """

    def remove_output_fields(posting_instruction_batches: List[Dict]):
        for pib in posting_instruction_batches:
            pib["client_id"] = MIGRATION_CLIENT_ID
            for key_to_remove in PIB_OUTPUT_FIELDS:
                pib.pop(key_to_remove, None)
            for pi in pib["posting_instructions"]:
                for key_to_remove in PI_OUTPUT_FIELDS:
                    pi.pop(key_to_remove, None)
        return posting_instruction_batches

    # remove matching PIBs to skip from posting instruction batches
    skipped_pibs, adjusted_res = skip_postings(simulation_setup, res)

    input_pibs = []
    if migrate_balance_ts:
        for result in adjusted_res:
            if not result.get("result"):
                raise KeyError(
                    f"No result found. Possible error in simulation {result}"
                )
            input_pibs.extend(
                remove_output_fields(result["result"]["posting_instruction_batches"])
            )
    else:
        balances = get_balances(res)["Main account"]
        final_ts, final_balances = balances.all()[-1]
        adjusted_balances = adjust_balances_from_skipped_pibs(
            account_id="Main account",
            balances=final_balances,
            skipped_balances=get_balances_from_pibs(skipped_pibs),
            tside=tside,
        )
        instructions = [
            CustomInstruction(
                postings=[
                    Posting(
                        account_id="Main account",
                        amount=str(abs(balance.net)),
                        credit=(balance.net > 0 and tside == "LIABILITY")
                        or (balance.net < 0 and tside == "ASSET"),
                        denomination=dimensions[2],
                        asset=dimensions[1],
                        account_address=dimensions[0],
                        phase=dimensions[3],
                    ),
                    Posting(
                        account_id="1",
                        amount=str(abs(balance.net)),
                        credit=(balance.net < 0 and tside == "LIABILITY")
                        or (balance.net > 0 and tside == "ASSET"),
                        denomination=dimensions[2],
                        asset=dimensions[1],
                        account_address=dimensions[0],
                        phase=dimensions[3],
                    ),
                ]
            )
            for dimensions, balance in adjusted_balances.items()
            if balance.net != 0
        ]

        input_pibs.append(
            create_posting_instruction_batch(
                instructions=instructions,
                value_datetime=final_ts,
                client_id=MIGRATION_CLIENT_ID,
            )["posting_instruction_batch"]
        )

    return input_pibs


def update_target_account(account_id: str, pib_template: Dict) -> Dict:
    """
    Updates the postings template with the desired account_id. Ensures that resulting postings:
    - debit/credit the right customer account
    - have unique-enough values where necessary (e.g. client_transaction_id)
    :param account_id: the account id to update the template with
    :param pib_template: the templated posting instruction batch to update
    :return: a pib with corresponding account_id
    """
    account_pib = copy.deepcopy(pib_template)
    account_pib["client_batch_id"] = str(uuid.uuid4())
    for pi in account_pib["posting_instructions"]:
        pi["client_transaction_id"] = str(uuid.uuid4())
        if pi.get("instruction_details", {}).get("originating_account_id"):
            pi["instruction_details"]["originating_account_id"] = account_id
        for valid_instruction in VALID_INSTRUCTIONS:
            if valid_instruction in pi:
                if valid_instruction == "custom_instruction":
                    for posting in pi[valid_instruction]["postings"]:
                        if posting["account_id"] == "Main account":
                            posting["account_id"] = account_id
                elif (
                    pi[valid_instruction]["target_account"]["account_id"]
                    == "Main account"
                ):
                    pi[valid_instruction].pop("target_account_id", None)
                    pi[valid_instruction]["target_account"]["account_id"] = account_id
    return account_pib


def populate_pib_timestamps(pib_templates: Dict):
    """
    Populate value timestamps from delta from config.
    """
    for pib in pib_templates:
        # the value_timestamp in config might be empty, in which case no value_timestamp should be
        # populated
        value_timestamp = pib.get("value_timestamp")
        if value_timestamp:
            pib["value_timestamp"] = extract_date(
                pib.get("value_timestamp")
            ).isoformat()


def generate_postings_template(
    product_name: str,
    simulation_client,
    simulation_contracts: Dict[str, ContractConfig],
    simulation_setup: Optional[Dict] = None,
) -> List[Dict]:
    """
    Generate a list of posting instruction batches to migrate for each account as part of
    performance test setup. The PIBs are generated by simulating the contract of a specific
    time window. This ensures a realistic historic dataset
    """

    if not simulation_setup:
        log.info("No simulation setup specified in test profile")
        return []

    log.info("Generating postings template")

    simulation_start = extract_date(simulation_setup["start"])
    simulation_end = extract_date(simulation_setup["end"])

    events = []
    for event in simulation_setup["events"]:
        event_type = event["type"]
        event.pop("type")

        if event.get("timestamp"):
            event["timestamp"] = extract_date(
                event["timestamp"], simulation_start, simulation_end
            )
        if event_type == "create_account_instruction":
            events.append(create_account_instruction(**event))
        elif event_type == "create_flag_definition_event":
            events.append(create_flag_definition_event(**event))
        elif event_type == "create_flag_event":
            event["expiry_timestamp"] = extract_date(
                event.get("expiry_timestamp"), simulation_start, simulation_end
            )
            events.append(create_flag_event(**event))
        elif event_type == "create_inbound_hard_settlement_instruction":
            event["event_datetime"] = extract_date(
                event.get("event_datetime"), simulation_start, simulation_end
            )
            events.append(create_inbound_hard_settlement_instruction(**event))
        elif event_type == "create_outbound_hard_settlement_instruction":
            event["event_datetime"] = extract_date(
                event.get("event_datetime"), simulation_start, simulation_end
            )
            events.append(create_outbound_hard_settlement_instruction(**event))

    res = simulation_client.simulate_smart_contract(
        contract_codes=[
            load_file_contents(contract.contract_file_path)
            for contract in simulation_contracts.values()
        ],
        start_timestamp=simulation_start,
        end_timestamp=simulation_end,
        templates_parameters=[
            contract.template_params for contract in simulation_contracts.values()
        ],
        smart_contract_version_ids=[
            contract.smart_contract_version_id
            for contract in simulation_contracts.values()
        ],
        events=events,
        contract_config=simulation_contracts[product_name],
    )

    migrate_balance_ts = simulation_setup.get("migrate_balance_ts", True)
    tside = simulation_setup.get("tside", "LIABILITY")
    postings_template = extract_postings_template(
        res,
        simulation_setup,
        tside,
        migrate_balance_ts,
    )

    log.info(f"Generated {len(postings_template)} templated postings")

    expected_number_of_postings = simulation_setup.get(
        "expected_number_of_postings", None
    )
    if expected_number_of_postings is not None and expected_number_of_postings != len(
        postings_template
    ):
        raise Exception(
            "Number of expected postings defined in profile setup does not match the "
            f"postings generated. expected_number_of_postings {expected_number_of_postings} "
            f"!= {len(postings_template)}"
        )

    return postings_template


def create_dataloader_resources(
    producer, dataloader_setup: Dict
) -> Dict[str, BatchResourceIds]:
    """
    Creates data loader resources by creating and publishing the dataloader resource batch requests
    :return: Dict of batch id to resource type to list of corresponding resource ids
    """

    log.info("Creating dataloader resources")

    if dataloader_setup.get("re_use_customers", False) is False:
        dependency_groups = determine_customer_id_bases(
            dataloader_setup["dependency_groups"]
        )
    else:
        dependency_groups = dataloader_setup["dependency_groups"]

    return endtoend.data_loader_helper.create_and_produce_data_loader_requests(
        producer,
        product_version_id=endtoend.contracts_helper.get_current_product_version_id(
            dataloader_setup["contract_name"]
        ),
        dependency_groups=dependency_groups,
        batch_size=100,
    )


def determine_customer_id_bases(dependency_groups: List[Dict]) -> List[Dict]:
    """
    Generates new customer_id_bases to prevent re-use of existing customers in an environment. See
    determine_customer_id_base for logic specifics.
    Assumes that any dependency groups with identical customer id_base intend to re-use customers
    across the groups, regardless of number of instances. For example, a first group with 100
    instances and id_base 1000000000 and a second group with 200 instances and id_base 1000000000
    would both be assigned the same new id_base, if required as per determine_customer_id_base.
    :param dependency_groups: the test profile's dependency_groups
    :return updated dependency groups
    """

    customer_id_bases = dict()

    # We want distinct id_bases in the profile and the largest corresponding number of customers
    for dependency_group in dependency_groups:
        customer_id_bases[dependency_group["customer"]["id_base"]] = max(
            int(dependency_group["instances"]),
            int(customer_id_bases.get(dependency_group["customer"]["id_base"], 0)),
        )

    # Overlapping customer ranges aren't technically a problem, but it's worth warning in case it
    # is not intentional
    overlapping_ranges = _get_range_overlaps(
        [
            range(id_base, id_base + number_of_customers)
            for id_base, number_of_customers in customer_id_bases.items()
        ]
    )
    for overlap in overlapping_ranges:
        log.warning(
            f"Customer ranges {overlapping_ranges[overlap[0]]} and "
            f"{overlapping_ranges[overlap[1]]} overlap"
        )

    new_id_bases = {
        original_base_id: determine_customer_id_base(
            number_of_customers, original_base_id
        )
        for original_base_id, number_of_customers in customer_id_bases.items()
    }

    updated_dependency_groups = copy.deepcopy(dependency_groups)
    for dependency_group in updated_dependency_groups:
        new_base = new_id_bases[dependency_group["customer"]["id_base"]]
        log.info(
            f'Updating customer id_base from {dependency_group["customer"]["id_base"]}'
            f" to {new_base}"
        )
        dependency_group["customer"]["id_base"] = new_base

    return updated_dependency_groups


def _get_range_overlaps(ranges: List[range]) -> List[Tuple[int, int]]:
    """
    Determine which ranges overlap. Two ranges A and B overlap iff their intersection is not empty
    AND neither range is a subset of the other.
    Assumes the step is 1 in all cases.
    :param ranges: the ranges to check for overlap
    :return: a list of tuples, each one corresponding to the pair of list indices that overlap
    """
    overlaps = []

    for i, current_range in enumerate(ranges):
        for j, next_range in enumerate(ranges[i + 1 :]):  # noqa: E203
            if (
                current_range.start < next_range.start
                and current_range.stop < next_range.stop
                and current_range.stop >= next_range.start
            ):
                overlaps.append((i, j + i + 1))

    return overlaps


def determine_customer_id_base(
    number_of_customers: int, original_base_id: int, max_increments=100
) -> int:
    """
    Get suitable customer_id base to create new customers without clashing with existing records.
    Assumes that all customers are created via the framework. original_base_id will be increased
    until no existing customers exist at base_id and base_id + number_of_customers
    """

    def customer_exists(cutomer_id):
        try:
            resp = endtoend.core_api_helper.get_customer(str(cutomer_id))
        except HTTPError as e:
            if "404 Client Error: Not Found for url" in e.args[0]:
                return False
            else:
                raise e
        else:
            try:
                msg = resp.get("message")
            except Exception:
                pass
            else:
                if msg is not None and msg == "User not found":
                    return False
        return True

    remaining_increments = max_increments
    while remaining_increments > 0:
        # if we haven't found a clean range try again with a new random number included
        start_id = int(
            str(original_base_id)
            + str(random.randint(1000000, 9999999))
            + str("00000000")
        )
        end_id = start_id + number_of_customers - 1
        start_id_found = customer_exists(start_id)
        end_id_found = customer_exists(end_id)

        if not (start_id_found or end_id_found):
            return start_id

        remaining_increments -= 1

    # If we get here we've failed to find a suitable base id
    raise Exception(
        f"Failed to determine a customer_id base after {max_increments} attempts. Please check"
        f" for a suitable range manually"
    )


def get_expected_balances(
    balances: List[Dict[str, str]]
) -> Dict[BalanceDimensions, ExpectedBalanceComparison]:
    """
    Construct the expected balances based on the input from the test_profile or alternatively
    use the given default values.
    """
    return (
        {
            BalanceDimensions(
                balance.get("address", "DEFAULT"),
                balance.get("asset", "COMMERCIAL_BANK_MONEY"),
                balance.get("denomination", "GBP"),
                balance.get("phase", "POSTING_PHASE_COMMITTED"),
            ): ExpectedBalanceComparison(
                net=Decimal(balance.get("net", "0")),
                credit=Decimal(balance.get("net_credits", "0")),
                debit=Decimal(balance.get("net_debits", "0")),
            )
            for balance in balances
        }
        if balances
        else None
    )


def _map_expected_workflow_ids_to_e2e_ids(
    expected_workflows: List[Dict[str, int]]
) -> Dict[str, int]:

    return (
        {
            endtoend.testhandle.workflow_definition_id_mapping[
                wf_id["workflow_definition_id"]
            ]: int(wf_id["number_of_instantiations"])
            for wf_id in expected_workflows
        }
        if expected_workflows
        else None
    )


def get_balances_from_pibs(
    posting_instruction_batches: List[Dict],
) -> Dict[Tuple, Decimal]:
    pib_balances = {}
    for posting_instruction_batch in posting_instruction_batches:
        for posting_instructions in posting_instruction_batch["posting_instructions"]:
            for posting in posting_instructions["committed_postings"]:
                if posting["phase"] == "POSTING_PHASE_COMMITTED":
                    amount = Decimal(posting["amount"])
                    if posting["credit"] is False:
                        amount = Decimal("-" + posting["amount"])
                    dimension = (
                        posting["account_id"],
                        posting["denomination"],
                        posting["account_address"],
                        posting["asset"],
                    )
                    pib_balances[dimension] = (
                        pib_balances[dimension] + amount
                        if dimension in pib_balances
                        else amount
                    )
    return pib_balances


def skip_postings(
    simulation_setup: Dict, res: List[Dict]
) -> Tuple[List[Dict], List[Dict]]:
    simulation_start = extract_date(simulation_setup["start"])
    simulation_end = extract_date(simulation_setup["end"])
    removed_pibs = []
    adjusted_res = copy.deepcopy(res)
    for result in adjusted_res:
        for pib_skip in simulation_setup.get("postings_to_skip", {}):
            skip_name = pib_skip.get("name")
            regex = pib_skip.get("client_batch_id_regex")
            if regex is None:
                log.warn(
                    f"Postings to skip '{skip_name}' is missing a client batch ID regex"
                )
                continue
            prog = re.compile(regex)
            skip_from = extract_date(
                date_entry=pib_skip.get("from"), start=simulation_start
            )
            skip_to = (
                extract_date(date_entry=pib_skip.get("to"), start=skip_from)
                if pib_skip.get("to")
                else simulation_end
            )
            for pib in result["result"]["posting_instruction_batches"]:
                client_batch_id = pib.get("client_batch_id")
                if client_batch_id is None:
                    log.warn(f"PIB is missing a client batch ID: {pib}")
                    continue
                value_timestamp = pib.get("value_timestamp")
                if value_timestamp is None:
                    log.warn(f"PIB is missing a value timestamp: {pib}")
                    continue
                else:
                    pib_datetime = extract_date(value_timestamp)
                if (
                    prog.match(client_batch_id)
                    and pib_datetime >= skip_from
                    and pib_datetime <= skip_to
                ):
                    result["result"]["posting_instruction_batches"].remove(pib)
                    removed_pibs.append(pib)
    return removed_pibs, adjusted_res


def adjust_balances_from_skipped_pibs(
    account_id: str,
    balances: DefaultDict[BalanceDimensions, Balance],
    skipped_balances: Dict[Tuple, Decimal],
    tside: str = "LIABILITY",
) -> Dict[BalanceDimensions, Balance]:
    adjusted_balances = {}
    for dimensions, balance in balances.items():
        adjusted_dimensions = copy.deepcopy(dimensions)
        adjusted_balance = copy.deepcopy(balance)
        adjusted_balances[adjusted_dimensions] = adjusted_balance
        if dimensions[3] == "POSTING_PHASE_COMMITTED":
            skipped_balance_dimensions = (
                account_id,
                dimensions[2],
                dimensions[0],
                dimensions[1],
            )
            if (
                skipped_balance_dimensions in skipped_balances
                and skipped_balances[skipped_balance_dimensions] != 0
            ):
                if tside == "LIABILITY":
                    adjusted_balance.net -= skipped_balances[skipped_balance_dimensions]
                else:
                    adjusted_balance.net += skipped_balances[skipped_balance_dimensions]
    return adjusted_balances


def generate_postings_for_accounts(
    account_ids: List[str],
    pib_template: List[Dict],
) -> Iterator[Dict]:
    postings_per_account = {account_id: 0 for account_id in account_ids}
    num_postings_template = len(pib_template)
    # Send each posting in order for each account, but
    # randomize overall order
    total_postings = num_postings_template * len(account_ids)
    for _ in range(total_postings):
        account_idx = random.randint(0, len(account_ids) - 1)
        account_id = account_ids[account_idx]

        posting_num = postings_per_account[account_id]
        yield update_target_account(account_id, pib_template[posting_num])

        postings_per_account[account_id] += 1
        if postings_per_account[account_id] >= num_postings_template:
            account_ids[account_idx] = account_ids[-1]
            account_ids.pop()


def get_account_update_events_consumer():
    return endtoend.testhandle.kafka_consumers[ACCOUNT_UPDATE_EVENTS_TOPIC]


def get_postings_responses_consumer():
    return endtoend.testhandle.kafka_consumers[MIGRATIONS_POSTINGS_RESPONSES_TOPIC]


def get_balance_updates_consumer():
    return endtoend.testhandle.kafka_consumers[ACCOUNT_BALANCE_EVENTS_TOPIC]
