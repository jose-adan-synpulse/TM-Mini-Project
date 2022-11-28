# standard libs
import json
import sys
import os
import unittest
import random
import time
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict
from unittest import TestCase
from unittest.mock import Mock, call, patch
from collections import defaultdict, deque
from copy import deepcopy

# third party
from requests import HTTPError

# common
from common.test_utils.common.balance_helpers import (
    BalanceDimensions,
    Balance,
    ExpectedBalanceComparison,
)
from common.test_utils.common.utils import create_mock_message_queue
from common.test_utils.contracts.simulation.data_objects.data_objects import (
    ContractConfig,
)
import common.test_utils.endtoend as endtoend
from common.test_utils.endtoend import data_loader_helper, core_api_helper, testhandle
from common.test_utils.performance import performance_helper
from common.test_utils.performance.performance_helper import (
    PerformanceTest,
    generate_postings_template,
    get_balances_from_pibs,
    skip_postings,
    adjust_balances_from_skipped_pibs,
)
from common.test_utils.performance.data_objects.data_objects import (
    ExpectedOutcomeValidationResults,
)
from common.test_utils.performance.test.unit.input.generate_postings_sim_input import (
    EVENTS,
)
from common.test_utils.performance.test.unit.input.posting_instruction_batches import (
    CURRENT_ACCOUNT_ACCRUE_PIBS_1,
    LOAN_PIBS_1,
)
from common.test_utils.performance.test.unit.output.postings_template import (
    POSTINGS_TEMPLATE,
)
from common.test_utils.performance.test.unit.output.sim_output import (
    SIM_RESULT,
    SIM_RESULT_CURRENT_ACCOUNT,
)

TEST_PROFILE = "common/test_utils/performance/test/unit/input/test_profile.yaml"
SAMPLE_BALANCE_MESSAGE = (
    "common/test_utils/performance/test/unit/input/sample_balance_update_event.json"
)
SAMPLE_WF_MESSAGE = (
    "common/test_utils/performance/test/unit/input/sample_wf_instantiation_event.json"
)


MOCK_COUNTER = 0


class PerformanceHelperTests(TestCase):
    def setUp(self) -> None:
        self.performance_test = PerformanceTest()
        self.performance_test.load_profile(TEST_PROFILE)
        return super().setUp()

    def test_subset_ranges_are_not_overlapping(self):
        range_1 = range(1, 5)
        range_2 = range(3, 5)
        range_3 = range(2, 4)
        self.assertEqual(
            performance_helper._get_range_overlaps([range_1, range_2, range_3]), []
        )

    def test_multiple_overlapping_ranges_detected(self):
        range_1 = range(1, 5)
        range_2 = range(3, 6)
        range_3 = range(4, 8)
        range_4 = range(10, 20)
        self.assertEqual(
            performance_helper._get_range_overlaps(
                [range_1, range_2, range_3, range_4]
            ),
            [(0, 1), (0, 2), (1, 2)],
        )

    def test_distinct_ranges_are_not_overlapping(self):
        range_1 = range(1, 5)
        range_2 = range(6, 10)
        self.assertEqual(performance_helper._get_range_overlaps([range_1, range_2]), [])

    def test_touching_ranges_are_overlapping(self):
        range_1 = range(1, 5)
        range_2 = range(5, 10)
        self.assertEqual(
            performance_helper._get_range_overlaps([range_1, range_2]), [(0, 1)]
        )

    @patch.object(core_api_helper, "get_customer")
    @patch.object(random, "randint")
    def test_customer_range_offset_if_only_start_customer_found(
        self, randint, get_customer
    ):
        start_base = 100
        num_customers = 100
        randint.return_value = 1000001
        get_customer.side_effect = [
            "a",  # Anything that isn't an exception counts as found customer
            HTTPError("404 Client Error: Not Found for url"),
            HTTPError("404 Client Error: Not Found for url"),
            HTTPError("404 Client Error: Not Found for url"),
        ]
        performance_helper.determine_customer_id_base(num_customers, start_base)
        get_customer.assert_has_calls(
            [
                call("100100000100000000"),
                call("100100000100000099"),
                call("100100000100000000"),
                call("100100000100000099"),
            ]
        )

    @patch.object(core_api_helper, "get_customer")
    @patch.object(random, "randint")
    def test_customer_range_offset_if_start_and_end_customer_found(
        self, randint, get_customer
    ):
        start_base = 100
        num_customers = 100
        randint.return_value = 1000001
        get_customer.side_effect = [
            "a",  # Anything that isn't an exception counts as found customer
            "b",  # Anything that isn't an exception counts as found customer
            HTTPError("404 Client Error: Not Found for url"),
            HTTPError("404 Client Error: Not Found for url"),
        ]
        performance_helper.determine_customer_id_base(num_customers, start_base)
        get_customer.assert_has_calls(
            [
                call("100100000100000000"),
                call("100100000100000099"),
                call("100100000100000000"),
                call("100100000100000099"),
            ]
        )

    @patch.object(core_api_helper, "get_customer")
    @patch.object(random, "randint")
    def test_customer_range_offset_if_only_end_customer_found(
        self, randint, get_customer
    ):
        start_base = 100
        num_customers = 100
        randint.return_value = 1000001
        get_customer.side_effect = [
            HTTPError("404 Client Error: Not Found for url"),
            "b",  # Anything that isn't an exception counts as found customer
            HTTPError("404 Client Error: Not Found for url"),
            HTTPError("404 Client Error: Not Found for url"),
        ]

        performance_helper.determine_customer_id_base(num_customers, start_base)
        get_customer.assert_has_calls(
            [
                call("100100000100000000"),
                call("100100000100000099"),
                call("100100000100000000"),
                call("100100000100000099"),
            ]
        )

    @patch.object(core_api_helper, "get_customer")
    def test_customer_range_not_offset_if_neither_customer_found(self, get_customer):
        start_base = 100
        get_customer.side_effect = [
            HTTPError("404 Client Error: Not Found for url"),
            HTTPError("404 Client Error: Not Found for url"),
        ]
        new_base = performance_helper.determine_customer_id_base(start_base, start_base)
        self.assertTrue(
            100100000000000000 <= new_base <= 100999999900000000,
            "new base id within expected range",
        )

    @patch.object(core_api_helper, "get_customer")
    def test_correct_customer_range_queried(self, get_customer: Mock):
        start_base = 100
        number_customers = 576
        get_customer.side_effect = [
            "a",
            HTTPError("404 Client Error: Not Found for url"),
            HTTPError("404 Client Error: Not Found for url"),
            HTTPError("404 Client Error: Not Found for url"),
        ]
        performance_helper.determine_customer_id_base(number_customers, start_base)

        self.assertEqual(len(get_customer.mock_calls), 4, "get_customer called 4 times")
        for mock_call in get_customer.mock_calls:
            self.assertTrue(
                100100000000000000 <= int(mock_call[1][0]) <= 100999999900000000,
                "call argument within expected range",
            )

    @patch(
        "common.test_utils.endtoend.testhandle.CONTRACTS",
        {"current_account": {"template_params": {}}},
    )
    def test_extract_contract_template_parameters(self):
        self.performance_test.extract_contract_template_parameters()
        self.assertEqual(
            testhandle.CONTRACTS["current_account"]["template_params"],
            {"minimum_balance_fee": "10"},
        )

    @patch(
        "common.test_utils.endtoend.testhandle.CONTRACTS",
        {"credit_card": {"template_params": {}}},
    )
    def test_extract_contract_template_parameters_contract_not_found(self):
        with self.assertRaises(Exception) as ex:
            self.performance_test.extract_contract_template_parameters()
        self.assertIn(
            "The contract in the test profile (current_account) could not be found. "
            "Please ensure the correct contract is loaded.",
            str(ex.exception),
        )

    @patch.object(endtoend, "extract_args")
    def test_extract_test_instances_updates_instances_from_cli(self, extract_args):
        args = Mock()
        args.instances = 25
        extract_args.return_value = (args, None)
        self.performance_test.extract_test_instances()
        dependency_group = self.performance_test.test_profile["dataloader_setup"][
            "dependency_groups"
        ][0]
        # Should equal value of args.instances
        self.assertEqual(dependency_group["instances"], 25)

    @patch.object(endtoend, "extract_args")
    def test_extract_test_instances_when_no_instances_from_cli(self, extract_args):
        args = Mock()
        args.instances = None
        extract_args.return_value = (args, None)
        self.performance_test.extract_test_instances()
        dependency_group = self.performance_test.test_profile["dataloader_setup"][
            "dependency_groups"
        ][0]
        # Should equal value of test_profile.yaml
        self.assertEqual(dependency_group["instances"], 1)

    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.core_api.v1.balances.account_balance.events": create_mock_message_queue(
                SAMPLE_BALANCE_MESSAGE
            ),
            "vault.api.v1.workflows.workflow_instance.create.requests": create_mock_message_queue(
                SAMPLE_WF_MESSAGE
            ),
        },
    )
    @patch.dict(
        "common.test_utils.endtoend.testhandle.workflow_definition_id_mapping",
        {"CURRENT_ACCOUNT_TEST_WF": "CURRENT_ACCOUNT_TEST_WF"},
    )
    def test_correct_validate_outcome(self):
        expected_outcome_detail = "No issues detected"
        (
            correct_output,
            detail,
        ) = self.performance_test.validate_expected_outcome(["1"], 1, 1)
        self.assertTrue(correct_output)
        self.assertEqual(detail, expected_outcome_detail)

    # TODO currently this is patching a Global variable (INTER_MESSAGE_TIMEOUT) but this could be
    # scoped to a PerformanceTest class attribute instead.
    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.core_api.v1.balances.account_balance.events": create_mock_message_queue(
                SAMPLE_BALANCE_MESSAGE
            ),
            "vault.api.v1.workflows.workflow_instance.create.requests": create_mock_message_queue(
                SAMPLE_WF_MESSAGE
            ),
        },
    )
    @patch.dict(
        "common.test_utils.endtoend.testhandle.workflow_definition_id_mapping",
        {"CURRENT_ACCOUNT_TEST_WF": "CURRENT_ACCOUNT_TEST_WF"},
    )
    def test_incorrect_validate_outcome(self):
        expected_outcome_detail = (
            f"Discrepancies found in expected outcome: "
            f"Invalid balances detected in 50.0% of accounts. Missing workflows in "
            f"50.0% of accounts. "
            f"See {os.getcwd()} for problem accounts."
        )
        (
            correct_output,
            detail,
        ) = self.performance_test.validate_expected_outcome(["1", "2"], 1, 1)
        self.assertFalse(correct_output)
        self.assertEqual(detail, expected_outcome_detail)

    def test_generate_postings_template(self):
        performance_helper.load_file_contents = Mock(return_value="file_contents")
        simulation_client = Mock()
        simulation_client.simulate_smart_contract = Mock(return_value=SIM_RESULT)
        simulation_contracts = {}
        simulation_contracts["current_account"] = ContractConfig(
            contract_file_path="/test/path",
            template_params={"param_1": "param_1"},
            account_configs=[],
            smart_contract_version_id="2",
        )
        postings_template = generate_postings_template(
            product_name="current_account",
            simulation_client=simulation_client,
            simulation_contracts=simulation_contracts,
            simulation_setup=self.performance_test.test_profile.get("simulation_setup"),
        )
        simulation_client.simulate_smart_contract.assert_called_with(
            contract_codes=["file_contents"],
            contract_config=ContractConfig(
                contract_file_path="/test/path",
                template_params={"param_1": "param_1"},
                account_configs=[],
                alias=None,
                smart_contract_version_id="2",
            ),
            end_timestamp=datetime(2020, 1, 11, 9, 0, tzinfo=timezone.utc),
            events=EVENTS,
            smart_contract_version_ids=["2"],
            start_timestamp=datetime(2020, 1, 10, 9, 0, tzinfo=timezone.utc),
            templates_parameters=[{"param_1": "param_1"}],
        )
        self.assertEqual(POSTINGS_TEMPLATE, postings_template)

    def test_get_balances_from_pibs(self):
        test_inputs = [
            {
                "description": "Balances from Current Account PIB",
                "posting_instruction_batches": deepcopy(CURRENT_ACCOUNT_ACCRUE_PIBS_1),
                "expected_balances": {
                    (
                        "Main account",
                        "GBP",
                        "INTERNAL_CONTRA",
                        "COMMERCIAL_BANK_MONEY",
                    ): Decimal("-0.00685"),
                    (
                        "Main account",
                        "GBP",
                        "ACCRUED_DEPOSIT",
                        "COMMERCIAL_BANK_MONEY",
                    ): Decimal("0.00685"),
                    ("1", "GBP", "DEFAULT", "COMMERCIAL_BANK_MONEY"): Decimal(
                        "0.00000"
                    ),
                },
            },
            {
                "description": "Balances from Loan PIB",
                "posting_instruction_batches": deepcopy(LOAN_PIBS_1),
                "expected_balances": {
                    (
                        "Main account",
                        "GBP",
                        "PRINCIPAL",
                        "COMMERCIAL_BANK_MONEY",
                    ): Decimal("-1000"),
                    ("1", "GBP", "DEFAULT", "COMMERCIAL_BANK_MONEY"): Decimal("1000"),
                },
            },
        ]
        for test_input in test_inputs:
            pib_balances = get_balances_from_pibs(
                test_input["posting_instruction_batches"]
            )
            self.assertEqual(
                pib_balances,
                test_input["expected_balances"],
                test_input["description"],
            )

    def test_skip_postings(self):
        simulation_setup = {
            "start": "2020-01-10 09:00:00 UTC",
            "end": "2020-01-13 09:00:00 UTC",
            "migrate_balance_ts": True,
            "expected_number_of_postings": 59,
            "postings_to_skip": [
                {
                    "name": "First accrual event",
                    "client_batch_id_regex": "^ACCRUE_INTEREST_AND_DAILY_FEES_.+$",
                    "from": {"delta": {"days": 0}},
                    "to": {"delta": {"days": 1}},
                }
            ],
        }
        test_inputs = [
            {
                "description": "Remove matching PIB",
                "simulation_setup": simulation_setup,
                "res": SIM_RESULT_CURRENT_ACCOUNT,
                "expected_num_removed_pibs": 1,
            },
            {
                "description": "Client batch ID does not match regex",
                "simulation_setup": simulation_setup,
                "sim_updates": {
                    "postings_to_skip": [
                        {
                            "name": "First accrual event",
                            "client_batch_id_regex": "^NONMATCH_ACCRUE_INTEREST_AND_DAILY_FEES_.+$",
                            "from": {"delta": {"days": 0}},
                            "to": {"delta": {"days": 1}},
                        }
                    ]
                },
                "res": SIM_RESULT_CURRENT_ACCOUNT,
                "expected_num_removed_pibs": 0,
            },
            {
                "description": "Value timestamp does not match",
                "simulation_setup": simulation_setup,
                "sim_updates": {
                    "postings_to_skip": [
                        {
                            "name": "First accrual event",
                            "client_batch_id_regex": "^ACCRUE_INTEREST_AND_DAILY_FEES_.+$",
                            "from": "2020-01-12 09:00:00 UTC",
                            "to": {"delta": {"days": 5}},
                        }
                    ]
                },
                "res": SIM_RESULT_CURRENT_ACCOUNT,
                "expected_num_removed_pibs": 0,
            },
            {
                "description": "Fixed to date in postings to skip",
                "simulation_setup": deepcopy(simulation_setup),
                "sim_updates": {
                    "postings_to_skip": [
                        {
                            "name": "First accrual event",
                            "client_batch_id_regex": "^ACCRUE_INTEREST_AND_DAILY_FEES_.+$",
                            "from": {"delta": {"days": 0}},
                            "to": "2020-01-11 09:00:00 UTC",
                        }
                    ]
                },
                "res": SIM_RESULT_CURRENT_ACCOUNT,
                "expected_num_removed_pibs": 1,
            },
        ]

        for test_input in test_inputs:
            simulation_res = test_input["res"]
            sim_setup = test_input["simulation_setup"]
            sim_updates = test_input.get("sim_updates")
            if sim_updates:
                sim_setup.update(sim_updates)
            removed_pibs, adjusted_res = skip_postings(sim_setup, simulation_res)
            self.assertEqual(
                len(removed_pibs),
                test_input["expected_num_removed_pibs"],
                test_input["description"],
            )

    def test_adjust_balances_from_skipped_pibs(self):
        test_inputs = [
            {
                "description": "Current account accrued deposit adjustment",
                "tside": "LIABILITY",
                "final_balances": {
                    BalanceDimensions(
                        address="DEFAULT",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("60"), net=Decimal("60")),
                    BalanceDimensions(
                        address="INTERNAL_CONTRA",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("0.22605"), net=Decimal("-0.22605")),
                    BalanceDimensions(
                        address="ACCRUED_DEPOSIT",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("0.22605"), net=Decimal("0.22605")),
                },
                "balance_deltas": {
                    (
                        "Main account",
                        "GBP",
                        "INTERNAL_CONTRA",
                        "COMMERCIAL_BANK_MONEY",
                    ): Decimal("-0.00685"),
                    (
                        "Main account",
                        "GBP",
                        "ACCRUED_DEPOSIT",
                        "COMMERCIAL_BANK_MONEY",
                    ): Decimal("0.00685"),
                    ("1", "GBP", "DEFAULT", "COMMERCIAL_BANK_MONEY"): Decimal(
                        "0.00000"
                    ),
                },
                "expected_final_balances": {
                    BalanceDimensions(
                        address="DEFAULT",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("60"), net=Decimal("60")),
                    BalanceDimensions(
                        address="INTERNAL_CONTRA",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("0.22605"), net=Decimal("-0.21920")),
                    BalanceDimensions(
                        address="ACCRUED_DEPOSIT",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("0.22605"), net=Decimal("0.21920")),
                },
            },
            {
                "description": "Empty balance deltas",
                "tside": "LIABILITY",
                "final_balances": {
                    BalanceDimensions(
                        address="DEFAULT",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("60"), net=Decimal("60")),
                    BalanceDimensions(
                        address="INTERNAL_CONTRA",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("0.22605"), net=Decimal("-0.22605")),
                    BalanceDimensions(
                        address="ACCRUED_DEPOSIT",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("0.22605"), net=Decimal("0.22605")),
                },
                "balance_deltas": {},
                "expected_final_balances": {
                    BalanceDimensions(
                        address="DEFAULT",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("60"), net=Decimal("60")),
                    BalanceDimensions(
                        address="INTERNAL_CONTRA",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("0.22605"), net=Decimal("-0.22605")),
                    BalanceDimensions(
                        address="ACCRUED_DEPOSIT",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("0.22605"), net=Decimal("0.22605")),
                },
            },
            {
                "description": "Loan with disbursal pib removed",
                "tside": "ASSET",
                "final_balances": {
                    BalanceDimensions(
                        address="PRINCIPAL",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("1000"), net=Decimal("1000")),
                    BalanceDimensions(
                        address="ACCRUED_EXPECTED_INTEREST",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("3.02848"), net=Decimal("3.02848")),
                    BalanceDimensions(
                        address="INTERNAL_CONTRA",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("6.05696"), net=Decimal("-6.05696")),
                    BalanceDimensions(
                        address="ACCRUED_INTEREST",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("3.02848"), net=Decimal("3.02848")),
                },
                "balance_deltas": {
                    (
                        "Main account",
                        "GBP",
                        "PRINCIPAL",
                        "COMMERCIAL_BANK_MONEY",
                    ): Decimal("-1000"),
                    ("1", "GBP", "DEFAULT", "COMMERCIAL_BANK_MONEY"): Decimal("1000"),
                },
                "expected_final_balances": {
                    BalanceDimensions(
                        address="PRINCIPAL",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("1000"), net=Decimal("0")),
                    BalanceDimensions(
                        address="ACCRUED_EXPECTED_INTEREST",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("3.02848"), net=Decimal("3.02848")),
                    BalanceDimensions(
                        address="INTERNAL_CONTRA",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(credit=Decimal("6.05696"), net=Decimal("-6.05696")),
                    BalanceDimensions(
                        address="ACCRUED_INTEREST",
                        asset="COMMERCIAL_BANK_MONEY",
                        denomination="GBP",
                        phase="POSTING_PHASE_COMMITTED",
                    ): Balance(debit=Decimal("3.02848"), net=Decimal("3.02848")),
                },
            },
        ]
        for test_input in test_inputs:
            new_balances = adjust_balances_from_skipped_pibs(
                "Main account",
                test_input["final_balances"],
                test_input["balance_deltas"],
                test_input["tside"],
            )
            self.assertEqual(
                new_balances,
                test_input["expected_final_balances"],
                test_input["description"],
            )

    @patch.object(performance_helper, "update_target_account")
    @patch.object(performance_helper, "create_and_produce_posting_request")
    @patch.object(performance_helper, "get_balance_updates_consumer")
    @patch.object(performance_helper, "get_postings_responses_consumer")
    @patch.object(performance_helper, "get_account_update_events_consumer")
    def test_accounts_postings_producer_loads_all_postings_on_multiple_accounts(
        self,
        mock_account_activations_consumer: Mock,
        mock_postings_responses_consumer: Mock,
        mock_balance_updates_consumer: Mock,
        create_and_produce_posting_request: Mock,
        update_target_account: Mock,
    ):
        # These mock functions are necessary, rather than using a list of side effects,
        # because the consumer threads are calling
        # the mocked functions tonnes of times during an execution.
        def mock_account_activations_consumer_decode():
            global MOCK_COUNTER
            MOCK_COUNTER += 1
            account = "ac" + str(MOCK_COUNTER)
            return (
                '{ "account_update_updated": { "account_update": { "account_id": "'
                + account
                + '", "status": "ACCOUNT_UPDATE_STATUS_COMPLETED" } } }'
            )

        def mock_postings_responses_consumer_decode():
            global MOCK_COUNTER
            MOCK_COUNTER += 1
            if MOCK_COUNTER > 3:
                MOCK_COUNTER = 1
            account = "ac" + str(MOCK_COUNTER)
            if MOCK_COUNTER == 1:
                # We need to delay the first time the postings reponses consumer returns
                # to allow time for the accounts first to become activated.
                time.sleep(0.5)
            return (
                '{"id": "pib_id_for_'
                + account
                + '", "create_request_id": "request_id_for_'
                + account
                + '", "status": "POSTING_INSTRUCTION_BATCH_STATUS_ACCEPTED"}'
            )

        def mock_balance_updates_consumer_decode():
            global MOCK_COUNTER
            MOCK_COUNTER += 1
            if MOCK_COUNTER > 3:
                MOCK_COUNTER = 1
            account = "ac" + str(MOCK_COUNTER)
            if MOCK_COUNTER == 1:
                # We need to delay the first time the balance updates consumer returns
                # to allow time for the postings responses.
                time.sleep(1)
            return (
                '{"account_id": "'
                + account
                + '", "posting_instruction_batch_id": "pib_id_for_'
                + account
                + '"}'
            )

        mock_account_activations_consumer().poll().value().decode.side_effect = (
            mock_account_activations_consumer_decode
        )
        mock_postings_responses_consumer().poll().value().decode.side_effect = (
            mock_postings_responses_consumer_decode
        )
        mock_balance_updates_consumer().poll().value().decode.side_effect = (
            mock_balance_updates_consumer_decode
        )
        mock_account_activations_consumer().poll().error.return_value = None
        mock_postings_responses_consumer().poll().error.return_value = None
        mock_balance_updates_consumer().poll().error.return_value = None
        create_and_produce_posting_request.side_effect = [
            "request_id_for_ac1",
            "request_id_for_ac2",
            "request_id_for_ac3",
        ]
        accounts_to_add = ["ac1", "ac2", "ac3"]
        postings_template_mock = Mock()
        postings_producer = performance_helper.AccountsPostingsProducer(
            account_ids=accounts_to_add,
            postings_template=[postings_template_mock],
        )
        postings_producer.create_postings()
        self.assertEqual(postings_producer.accounts_waiting_activation, deque())
        self.assertTrue(postings_producer.idle_accounts.empty())
        self.assertTrue(postings_producer.processing_complete.is_set())
        self.assertEqual(postings_producer.accounts_in_progress, {})
        self.assertEqual(postings_producer.accounts_to_process, deque())
        # postings producer accounts_loaded will only have the full list of accounts if all
        # threads are working in concert with expected values returned by the consumers
        self.assertEqual(
            accounts_to_add.sort(), postings_producer.results.accounts_loaded.sort()
        )

    @patch("logging.Logger.warning")
    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.core_api.v1.accounts.account_update.events": create_mock_message_queue(
                SAMPLE_BALANCE_MESSAGE
            ),
        },
    )
    def test_account_activations_consumer_times_out(
        self,
        warning_logging: Mock,
    ):
        accounts_to_add = ["ac1", "ac2", "ac3"]
        postings_template_mock = Mock()
        postings_producer = performance_helper.AccountsPostingsProducer(
            account_ids=accounts_to_add,
            postings_template=[postings_template_mock],
        )
        postings_producer.message_timeout = 1

        postings_producer._account_activations_consumer()
        self.assertEqual(
            postings_producer.accounts_waiting_activation, deque(accounts_to_add)
        )
        warning_logging.assert_called_with(
            f"Waited 2.0s since last account activation received. "
            f"Timeout set to 1.0. Exiting after {len(accounts_to_add)} accounts unactivated. "
            f"The unactivated account ids were: deque({accounts_to_add})"
        )

    def test_configure_postings_stages(self):
        stages = self.performance_test._configure_postings_stages()
        self.assertEqual(3, len(stages))
        self.assertEqual(
            [
                {
                    "tps": 5,
                    "duration": 30,
                    "timeout": 600,
                    "accounts_required": 75,
                },
                {
                    "tps": 10,
                    "duration": 30,
                    "timeout": 600,
                    "accounts_required": 150,
                },
                {
                    "tps": 15,
                    "duration": 30,
                    "timeout": 600,
                    "accounts_required": 225,
                },
            ],
            stages,
        )
        dependency_group = self.performance_test.test_profile["dataloader_setup"][
            "dependency_groups"
        ][0]
        # Should equal max value of accounts_required
        self.assertEqual(dependency_group["instances"], 225)

    def test_generate_postings_for_accounts(self):
        account_ids = [f"account_{i}" for i in range(10)]
        postings_iterator = performance_helper.generate_postings_for_accounts(
            account_ids,
            self.performance_test.test_profile["postings_setup"]["pib_template"],
        )
        postings = [p for p in postings_iterator]
        self.assertEqual(20, len(postings))

        # Check the postings are ordered per account
        postings_bytes = [json.dumps(pib) for pib in postings]
        txn_id_order = ["123456", "654321"]
        for account_id in account_ids:
            count = 0
            for pib in postings_bytes:
                if account_id in pib:
                    self.assertTrue(txn_id_order[count] in pib)
                    count += 1
            self.assertEqual(count, 2)

    def test_check_environment_sufficient(self):
        test_cases = [
            {
                "env": "skyfall_inception",
                "instances": 100,
                "exception": False,
            },
            {
                "env": "inception_ephemeral",
                "instances": 100,
                "exception": True,
            },
            {
                "env": "inception_ephemeral",
                "instances": 9,
                "exception": False,
            },
        ]

        dl_setup = self.performance_test.test_profile["dataloader_setup"]

        for test_case in test_cases:
            dl_setup["dependency_groups"][0]["instances"] = test_case["instances"]
            with patch(
                "common.test_utils.endtoend.testhandle.environment", test_case["env"]
            ):
                if test_case["exception"]:
                    self.assertRaises(
                        Exception,
                        self.performance_test._check_environment_sufficient,
                    )
                else:
                    self.performance_test._check_environment_sufficient()


SUCCESSFUL_BATCH = (
    "common/test_utils/performance/test/unit/output/successful_batch.json"
)
FAILED_BATCH = "common/test_utils/performance/test/unit/output/failed_batch.json"
PARTIALLY_FAILED_BATCH = (
    "common/test_utils/performance/test/unit/output/partially_failed_batch.json"
)
BATCH_ID = "ad221059-919b-49c4-8a87-745117f545e7"
BATCH_ACCOUNT_IDS = [
    "35300_3Y7OVNXC5LWUJXGL9TXBOFZW208ER",
    "35301_3Y7OVNXC5LWUJXGL9TXBOFZW208ER",
    "35302_3Y7OVNXC5LWUJXGL9TXBOFZW208ER",
]
BATCH_CUSTOMER_IDS = ["60004000000115300", "60004000000115301", "60004000000115302"]
# Failed order is flipped due to sort/group_by. No functional impact so amending the test asserts
# instead of adding further sort to the code
FAILED_BATCH_ACCOUNT_IDS = [
    "35302_3Y7OVNXC5LWUJXGL9TXBOFZW208ER",
    "35300_3Y7OVNXC5LWUJXGL9TXBOFZW208ER",
]
FAILED_BATCH_CUSTOMER_IDS = ["60004000000115302", "60004000000115300"]


class BatchHandlerTests(TestCase):

    successful_batch: Dict
    failed_batch: Dict
    partially_failed_batch: Dict

    @classmethod
    def setUpClass(cls):
        with open(SUCCESSFUL_BATCH, encoding="utf-8") as file:
            cls.successful_batch = json.load(file)
        with open(FAILED_BATCH, encoding="utf-8") as file:
            cls.failed_batch = json.load(file)
        with open(PARTIALLY_FAILED_BATCH, encoding="utf-8") as file:
            cls.partially_failed_batch = json.load(file)

    def setUp(self) -> None:

        self.batched_ids = data_loader_helper.BatchResourceIds(
            account_ids=BATCH_ACCOUNT_IDS, customer_ids=BATCH_CUSTOMER_IDS, flag_ids=[]
        )

        return super().setUp()

    def test_all_resources_in_successful_batch_marked_as_loaded(self):

        handler = performance_helper.BatchUpdatedHandler(
            {BATCH_ID: self.batched_ids},
        )

        handler._handle_batch(self.successful_batch)
        self.assertListEqual(handler.loaded_accounts, BATCH_ACCOUNT_IDS)
        self.assertListEqual(handler.loaded_customers, BATCH_CUSTOMER_IDS)

    @patch.object(data_loader_helper, "batch_get_resource_batches")
    def test_failed_batch_resources_in_successful_dependency_groups_marked_as_loaded(
        self, batch_get_resource_batches: Mock
    ):

        batch_get_resource_batches.side_effect = [
            self.partially_failed_batch,
        ]

        handler = performance_helper.BatchUpdatedHandler(
            {BATCH_ID: self.batched_ids},
        )
        handler._handle_batch(self.partially_failed_batch[BATCH_ID])
        self.assertListEqual(handler.loaded_accounts, FAILED_BATCH_ACCOUNT_IDS)
        self.assertListEqual(handler.loaded_customers, FAILED_BATCH_CUSTOMER_IDS)

    @patch.object(data_loader_helper, "batch_get_resource_batches")
    def test_failed_batch_status_updated_to_partially_recovered_if_it_contains_loaded_accounts(
        self, batch_get_resource_batches: Mock
    ):

        batch_get_resource_batches.side_effect = [
            self.partially_failed_batch,
        ]

        handler = performance_helper.BatchUpdatedHandler(
            {BATCH_ID: self.batched_ids},
        )
        handler._handle_batch(self.partially_failed_batch[BATCH_ID])
        self.assertEqual(handler.unsuccessful_batches[BATCH_ID], "PARTIALLY_RECOVERED")

    @patch.object(data_loader_helper, "batch_get_resource_batches")
    def test_failed_batch_not_marked_as_recovered_if_error_when_retrieving(
        self, batch_get_resource_batches: Mock
    ):

        batch_get_resource_batches.side_effect = [HTTPError]

        handler = performance_helper.BatchUpdatedHandler(
            {BATCH_ID: self.batched_ids},
        )
        handler._handle_batch(self.partially_failed_batch[BATCH_ID])
        self.assertNotIn(BATCH_ID, handler.unsuccessful_batches)
        self.assertNotIn(BATCH_ID, handler.successful_batches)

    @patch.object(data_loader_helper, "batch_get_resource_batches")
    def test_failed_batch_status_not_updated_if_it_contains_no_loaded_accounts(
        self, batch_get_resource_batches: Mock
    ):

        batch_get_resource_batches.side_effect = [
            self.failed_batch,
        ]

        handler = performance_helper.BatchUpdatedHandler(
            {BATCH_ID: self.batched_ids},
        )
        handler._handle_batch(self.failed_batch[BATCH_ID])
        self.assertEqual(
            handler.unsuccessful_batches[BATCH_ID], "RESOURCE_BATCH_STATUS_FAILED"
        )

    @patch.object(data_loader_helper, "batch_get_resource_batches")
    def test_postings_sent_for_loaded_accounts_in_successful_batch(
        self, batch_get_resource_batches: Mock
    ):

        batch_get_resource_batches.side_effect = [
            self.partially_failed_batch,
        ]

        handler = performance_helper.BatchUpdatedHandler(
            {BATCH_ID: self.batched_ids},
        )
        handler._handle_batch(self.successful_batch)

    @patch.object(data_loader_helper, "batch_get_resource_batches")
    def test_postings_sent_for_loaded_accounts_in_failed_batch(
        self, batch_get_resource_batches: Mock
    ):

        batch_get_resource_batches.side_effect = [
            self.partially_failed_batch,
        ]

        handler = performance_helper.BatchUpdatedHandler(
            {BATCH_ID: self.batched_ids},
        )
        handler._handle_batch(self.partially_failed_batch[BATCH_ID])

    @patch.object(data_loader_helper, "batch_get_resource_batches")
    def test_recovered_and_successful_batches_are_skipped_when_checking_missing_events(
        self, batch_get_resource_batches: Mock
    ):

        handler = performance_helper.BatchUpdatedHandler(
            {"id_1": self.batched_ids, "id_2": self.batched_ids},
        )
        handler.successful_batches.append("id_1")
        handler.unsuccessful_batches.update({"id_2": "PARTIALLY_RECOVERED"})
        handler.handle_missing_events()

        batch_get_resource_batches.assert_not_called()

    @patch.object(data_loader_helper, "batch_get_resource_batches")
    def test_missed_batches_handled_when_checking_missing_events(
        self, batch_get_resource_batches: Mock
    ):

        handler = performance_helper.BatchUpdatedHandler(
            {
                BATCH_ID: self.batched_ids,
            },
        )
        batch_get_resource_batches.side_effect = [
            self.partially_failed_batch,
        ]

        handler.handle_missing_events()

        self.assertListEqual(handler.loaded_accounts, FAILED_BATCH_ACCOUNT_IDS)
        self.assertListEqual(handler.loaded_customers, FAILED_BATCH_CUSTOMER_IDS)

    @patch.object(data_loader_helper, "batch_get_resource_batches")
    def test_truly_missing_batch_status_is_updated(
        self, batch_get_resource_batches: Mock
    ):

        handler = performance_helper.BatchUpdatedHandler(
            {
                "id_1": self.batched_ids,
                BATCH_ID: self.batched_ids,
            },
        )
        batch_get_resource_batches.side_effect = [
            HTTPError("404 Client Error: Not Found for url")
        ]

        # Make sure there is a successful batch + resource to avoid exception scenario
        handler.successful_batches.append("id_1")

        handler.handle_missing_events()
        self.assertEqual(handler.unsuccessful_batches[BATCH_ID], "MISSING_BATCH_EVENT")

    @patch.dict(
        "common.test_utils.endtoend.testhandle.workflow_definition_id_mapping",
        {
            "TEST_WF_NAME_1": "TEST_WF_NAME_1_e2e_MAPPING",
            "TEST_WF_NAME_2": "TEST_WF_NAME_2_e2e_MAPPING",
        },
    )
    def test_map_expected_workflow_ids_to_e2e_ids(self):

        expected_wfs_from_profile = [
            {"workflow_definition_id": "TEST_WF_NAME_1", "number_of_instantiations": 1},
            {"workflow_definition_id": "TEST_WF_NAME_2", "number_of_instantiations": 5},
        ]

        expected_result = {
            "TEST_WF_NAME_1_e2e_MAPPING": 1,
            "TEST_WF_NAME_2_e2e_MAPPING": 5,
        }

        result = performance_helper._map_expected_workflow_ids_to_e2e_ids(
            expected_wfs_from_profile
        )

        self.assertEqual(result, expected_result)


class ExpectedOutcomeConsumerTests(TestCase):
    def setUp(self) -> None:
        self.expected_balances = {
            BalanceDimensions(
                "DEFAULT", "COMMERCIAL_BANK_MONEY", "GBP", "POSTING_PHASE_COMMITTED"
            ): ExpectedBalanceComparison(
                net=Decimal("60"), credit=Decimal("60"), debit=Decimal("0")
            ),
            BalanceDimensions(
                "OTHER", "COMMERCIAL_BANK_MONEY", "GBP", "POSTING_PHASE_COMMITTED"
            ): ExpectedBalanceComparison(
                net=Decimal("20"), credit=Decimal("20"), debit=Decimal("0")
            ),
        }
        self.expected_workflows = {"CURRENT_ACCOUNT_TEST_WF": 1}
        return super().setUp()

    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.core_api.v1.balances.account_balance.events": create_mock_message_queue(
                SAMPLE_BALANCE_MESSAGE
            ),
            "vault.api.v1.workflows.workflow_instance.create.requests": create_mock_message_queue(
                SAMPLE_WF_MESSAGE
            ),
        },
    )
    def test_listen_to_consumers_expected_wf_and_expected_balances_valid(self):

        consumer = performance_helper.ExpectedOutcomeConsumer(
            account_ids=["1"],
            expected_balances=self.expected_balances,
            expected_workflows=self.expected_workflows,
        )

        consumer.listen_to_consumers(1, 1)

        self.assertIsInstance(consumer.results, ExpectedOutcomeValidationResults)
        self.assertEqual(consumer.results.accounts_with_incorrect_balances, {})
        self.assertEqual(consumer.results.accounts_with_missing_workflows, {})

    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.core_api.v1.balances.account_balance.events": create_mock_message_queue(
                SAMPLE_BALANCE_MESSAGE
            ),
            "vault.api.v1.workflows.workflow_instance.create.requests": create_mock_message_queue(
                SAMPLE_WF_MESSAGE
            ),
        },
    )
    def test_listen_to_consumers_expected_wf_and_expected_balances_invalid(self):

        missing_workflows = {
            "2": {"CURRENT_ACCOUNT_TEST_WF": {"expected": 1, "seen": 0}}
        }
        incorrect_balances = defaultdict(dict)
        incorrect_balances["2"] = {
            BalanceDimensions(
                "DEFAULT", "COMMERCIAL_BANK_MONEY", "GBP", "POSTING_PHASE_COMMITTED"
            ): "No balance updates seen",
            BalanceDimensions(
                "OTHER", "COMMERCIAL_BANK_MONEY", "GBP", "POSTING_PHASE_COMMITTED"
            ): "No balance updates seen",
        }

        consumer = performance_helper.ExpectedOutcomeConsumer(
            account_ids=["1", "2"],
            expected_balances=self.expected_balances,
            expected_workflows=self.expected_workflows,
        )

        consumer.listen_to_consumers(1, 1)

        self.assertIsInstance(consumer.results, ExpectedOutcomeValidationResults)
        self.assertDictEqual(
            consumer.results.accounts_with_incorrect_balances, incorrect_balances
        )
        self.assertEqual(
            consumer.results.accounts_with_missing_workflows, missing_workflows
        )

    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.core_api.v1.balances.account_balance.events": create_mock_message_queue(
                SAMPLE_BALANCE_MESSAGE
            ),
            "vault.api.v1.workflows.workflow_instance.create.requests": create_mock_message_queue(
                SAMPLE_WF_MESSAGE
            ),
        },
    )
    def test_listen_to_consumers_invalid_expected_credits_and_debits(self):

        expected_balances = {
            BalanceDimensions(
                "DEFAULT", "COMMERCIAL_BANK_MONEY", "GBP", "POSTING_PHASE_COMMITTED"
            ): ExpectedBalanceComparison(
                net=Decimal("60"), credit=Decimal("50"), debit=Decimal("50")
            ),
            BalanceDimensions(
                "OTHER", "COMMERCIAL_BANK_MONEY", "GBP", "POSTING_PHASE_COMMITTED"
            ): ExpectedBalanceComparison(
                net=Decimal("20"), credit=Decimal("50"), debit=Decimal("50")
            ),
        }

        incorrect_balances = defaultdict(dict)
        incorrect_balances["1"] = {
            BalanceDimensions(
                "DEFAULT", "COMMERCIAL_BANK_MONEY", "GBP", "POSTING_PHASE_COMMITTED"
            ): {
                "expected": {
                    "net": Decimal("60"),
                    "credit": Decimal("50"),
                    "debit": Decimal("50"),
                },
                "actual": {
                    "net": Decimal("60"),
                    "credit": Decimal("60"),
                    "debit": Decimal("0"),
                },
                "timestamp": datetime(1970, 1, 1, 0, 0),
            },
            BalanceDimensions(
                "OTHER", "COMMERCIAL_BANK_MONEY", "GBP", "POSTING_PHASE_COMMITTED"
            ): {
                "expected": {
                    "net": Decimal("20"),
                    "credit": Decimal("50"),
                    "debit": Decimal("50"),
                },
                "actual": {
                    "net": Decimal("20"),
                    "credit": Decimal("20"),
                    "debit": Decimal("0"),
                },
                "timestamp": datetime(1970, 1, 1, 0, 0),
            },
        }

        consumer = performance_helper.ExpectedOutcomeConsumer(
            account_ids=["1"],
            expected_balances=expected_balances,
            expected_workflows=None,
        )

        consumer.listen_to_consumers(1, 1)

        self.assertIsInstance(consumer.results, ExpectedOutcomeValidationResults)
        self.assertDictEqual(
            consumer.results.accounts_with_incorrect_balances, incorrect_balances
        )
        self.assertEqual(consumer.results.accounts_with_missing_workflows, {})

    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.core_api.v1.balances.account_balance.events": create_mock_message_queue(
                SAMPLE_BALANCE_MESSAGE
            ),
            "vault.api.v1.workflows.workflow_instance.create.requests": create_mock_message_queue(
                SAMPLE_WF_MESSAGE
            ),
        },
    )
    def test_listen_to_consumers_expected_wf_and_no_expected_balances_valid(self):

        consumer = performance_helper.ExpectedOutcomeConsumer(
            account_ids=["1"],
            expected_balances=None,
            expected_workflows=self.expected_workflows,
        )

        consumer.listen_to_consumers(1, 1)

        self.assertIsInstance(consumer.results, ExpectedOutcomeValidationResults)
        self.assertEqual(consumer.results.accounts_with_incorrect_balances, {})
        self.assertEqual(consumer.results.accounts_with_missing_workflows, {})

    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.core_api.v1.balances.account_balance.events": create_mock_message_queue(
                SAMPLE_BALANCE_MESSAGE
            ),
            "vault.api.v1.workflows.workflow_instance.create.requests": create_mock_message_queue(
                SAMPLE_WF_MESSAGE
            ),
        },
    )
    def test_listen_to_consumers_expected_balances_and_no_expected_wf_valid(self):

        consumer = performance_helper.ExpectedOutcomeConsumer(
            account_ids=["1"],
            expected_balances=self.expected_balances,
            expected_workflows=None,
        )

        consumer.listen_to_consumers(1, 1)

        self.assertIsInstance(consumer.results, ExpectedOutcomeValidationResults)
        self.assertEqual(consumer.results.accounts_with_incorrect_balances, {})
        self.assertEqual(consumer.results.accounts_with_missing_workflows, {})


if __name__ == "__main__":
    if any(item.startswith("test") for item in sys.argv[1:]):
        unittest.main(BatchHandlerTests)
    else:
        unittest.main()
