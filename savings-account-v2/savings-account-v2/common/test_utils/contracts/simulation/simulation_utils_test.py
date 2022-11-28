# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# standard libs
import io
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import call, patch

# common
from common.test_utils.common.balance_helpers import BalanceDimensions
from common.test_utils.contracts.simulation import simulation_test_utils
from common.test_utils.contracts.simulation.helper import (
    create_account_instruction,
    create_inbound_hard_settlement_instruction,
    create_outbound_hard_settlement_instruction,
)
from common.test_utils.contracts.simulation.data_objects.data_objects import (
    ExpectedDerivedParameter,
    ExpectedSchedule,
    ExpectedRejection,
    ExpectedWorkflow,
    SimulationTestScenario,
    SubTest,
    SuperviseeConfig,
    SupervisorConfig,
)


SIMULATOR_RESPONSE_FILE = (
    "common/test_utils/contracts/simulation/sample_simulator_response"
)
BACKDATED_SIMULATOR_RESPONSE_FILE = (
    "common/test_utils/contracts/simulation/" "backdated_simulator_response"
)
SUPERVISOR_RESPONSE_FILE = (
    "common/test_utils/contracts/simulation/sample_supervisor_response"
)


class UtilsTest(unittest.TestCase):
    """
    This tests the integration test utils.
    """

    def setUp(self):
        sim_res_file = SIMULATOR_RESPONSE_FILE
        supervisor_res_file = SUPERVISOR_RESPONSE_FILE
        backdated_sim_res_file = BACKDATED_SIMULATOR_RESPONSE_FILE

        with open(sim_res_file, "r", encoding="utf-8") as simulator_response_file:
            self.sample_res = eval(simulator_response_file.read())

        with open(
            backdated_sim_res_file, "r", encoding="utf-8"
        ) as simulator_response_file:
            self.backdated_sample_res = eval(simulator_response_file.read())

        with open(
            supervisor_res_file, "r", encoding="utf-8"
        ) as simulator_response_file:
            self.supervisor_res = eval(simulator_response_file.read())

    def test_get_account_notes(self):
        account_notes = simulation_test_utils.get_account_notes(
            res=self.sample_res,
        )

        for note in account_notes:
            self.assertIn("_INTEREST", note["body"])
            self.assertEqual("Main account", note["account_id"])

    def test_get_balances_for_undefined_dimensions(self):
        balances = simulation_test_utils.get_balances(res=self.sample_res)
        internal_balances = balances["1"]

        date = datetime(2019, 1, 1, minute=1, tzinfo=timezone.utc)
        self.assertEqual(
            internal_balances.before(date)[BalanceDimensions("XYZ")].net, Decimal("0")
        )

    def test_get_balances_for_undefined_dimensions_with_backdating(self):
        balances = simulation_test_utils.get_balances(
            res=self.sample_res, return_latest_event_timestamp=False
        )
        main_balances = balances["Main account"]

        date = datetime(2019, 1, 1, minute=1, tzinfo=timezone.utc)
        date2 = datetime(2019, 2, 1, minute=1, tzinfo=timezone.utc)
        self.assertEqual(
            main_balances.at(date)[BalanceDimensions(address="XYZ")].at(date2).net,
            Decimal("0"),
        )

    def test_get_balances_for_undefined_account(self):
        balances = simulation_test_utils.get_balances(res=self.sample_res)
        xyz_balances = balances["xyz"]
        date = datetime(2019, 1, 1, minute=1, tzinfo=timezone.utc)
        self.assertEqual(xyz_balances.at(date)[BalanceDimensions()].net, Decimal("0"))

    def test_get_balances_for_undefined_date(self):
        balances = simulation_test_utils.get_balances(res=self.sample_res)
        main_balances = balances["Main account"]
        date = datetime(2018, 1, 1, minute=1, tzinfo=timezone.utc)
        self.assertEqual(main_balances.at(date)[BalanceDimensions()].net, Decimal("0"))

    def test_get_balances_before_without_backdating(self):
        balances = simulation_test_utils.get_balances(res=self.sample_res)
        main_balances = balances["Main account"]
        internal_balances = balances["1"]

        date = datetime(2019, 1, 1, minute=1, tzinfo=timezone.utc)
        self.assertEqual(
            main_balances.before(date)[BalanceDimensions()].net, Decimal("-110")
        )
        self.assertEqual(
            internal_balances.before(date)[BalanceDimensions()].net, Decimal("110")
        )

    def test_get_balances_at_without_backdating(self):
        balances = simulation_test_utils.get_balances(res=self.sample_res)
        main_balances = balances["Main account"]
        internal_balances = balances["1"]

        date = datetime(2019, 1, 1, minute=1, tzinfo=timezone.utc)
        self.assertEqual(
            main_balances.at(date)[BalanceDimensions()].net, Decimal("-130")
        )
        self.assertEqual(
            internal_balances.at(date)[BalanceDimensions()].net, Decimal("130")
        )

    def test_get_balances_latest_without_backdating(self):
        balances = simulation_test_utils.get_balances(res=self.sample_res)
        main_balances = balances["Main account"]
        internal_balances = balances["1"]

        self.assertEqual(
            main_balances.latest()[BalanceDimensions()].net, Decimal("1030")
        )
        self.assertEqual(
            internal_balances.latest()[BalanceDimensions()].net, Decimal("-1030")
        )

    def test_get_balances_before_with_backdating(self):
        balances = simulation_test_utils.get_balances(
            res=self.backdated_sample_res, return_latest_event_timestamp=False
        )
        main_balances = balances["Main account"]

        event_date = datetime(2019, 1, 1, 2, tzinfo=timezone.utc)
        value_date = datetime(2019, 1, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(
            main_balances.at(value_date)[BalanceDimensions()].before(event_date).net,
            Decimal("300"),
        )

    def test_get_balances_at_with_backdating(self):
        balances = simulation_test_utils.get_balances(
            res=self.backdated_sample_res, return_latest_event_timestamp=False
        )
        main_balances = balances["Main account"]

        event_date_1 = datetime(2019, 1, 1, 1, tzinfo=timezone.utc)
        event_date_2 = datetime(2019, 1, 1, 2, tzinfo=timezone.utc)
        value_date = datetime(2019, 1, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(
            main_balances.at(value_date)[BalanceDimensions()].at(event_date_1).net,
            Decimal("300"),
        )
        self.assertEqual(
            main_balances.at(value_date)[BalanceDimensions()].at(event_date_2).net,
            Decimal("600"),
        )

    def test_get_balances_latest_with_backdating(self):
        balances = simulation_test_utils.get_balances(
            res=self.backdated_sample_res, return_latest_event_timestamp=False
        )
        main_balances = balances["Main account"]

        value_date = datetime(2019, 1, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(
            main_balances.at(value_date)[BalanceDimensions()].latest().net,
            Decimal("600"),
        )

    def test_get_flag_definition_created(self):
        is_flag_created = simulation_test_utils.get_flag_definition_created(
            res=self.sample_res, flag_definition_id="debug_flag"
        )

        self.assertTrue(is_flag_created)

    def test_get_flag_created(self):
        is_flag_applied_to_account = simulation_test_utils.get_flag_created(
            res=self.sample_res,
            flag_definition_id="debug_flag",
            account_id="Main account",
        )

        self.assertTrue(is_flag_applied_to_account)

    def test_get_logs(self):
        logs = simulation_test_utils.get_logs(res=self.sample_res)

        self.assertIn("created account", "".join(logs))
        self.assertIn("processed scheduled event", "".join(logs))

    def test_get_account_logs(self):
        account_logs_main = simulation_test_utils.get_account_logs(
            res=self.sample_res, account_id="Main account"
        )

        self.assertNotIn('account "1"', account_logs_main)
        self.assertIn('account "Main account"', account_logs_main)

        account_logs_1 = simulation_test_utils.get_account_logs(
            res=self.sample_res, account_id="1"
        )

        self.assertNotIn('account "Main account"', account_logs_1)
        self.assertIn('account "1"', account_logs_1)
        account_logs_supervisor = simulation_test_utils.get_account_logs(
            res=self.supervisor_res, account_id="Savings Account"
        )

        self.assertNotIn('account "Checking Account"', account_logs_supervisor)
        self.assertIn('account "Savings Account"', account_logs_supervisor)

    def test_create_supervisor_config(self):
        supervisee_1 = SuperviseeConfig(
            contract_id="contract_id_1",
            contract_file="contract_file_1",
            account_name="Account 1",
            version="1",
            instance_parameters={"a": "1"},
            template_parameters={"b": "2"},
            instances=3,
        )

        supervisee_2 = SuperviseeConfig(
            contract_id="contract_id_2",
            contract_file="contract_file_2",
            account_name="Account 1",
            version="2",
            instance_parameters={"c": "3"},
            template_parameters={"d": "4"},
            instances=4,
        )

        supervisor_contract = "supervisor_contract"
        supervisor_contract_version_id = "supervisor_contract_version_id"

        supervisor_config = simulation_test_utils.create_supervisor_config(
            supervisor_contract=supervisor_contract,
            supervisor_contract_version_id=supervisor_contract_version_id,
            supervisees=[supervisee_1, supervisee_2],
        )

        # check supervisor config
        self.assertIsInstance(supervisor_config, SupervisorConfig)

        self.assertEqual(supervisor_config.supervisor_file_path, supervisor_contract)
        self.assertEqual(
            supervisor_config.supervisor_contract_version_id,
            supervisor_contract_version_id,
        )

        # check supervisee contracts
        self.assertEqual(len(supervisor_config.supervisee_contracts), 2)

        # check supervisee contract configs
        expected_supervisee_contracts = [supervisee_1, supervisee_2]
        for i, supervisee_contract in enumerate(supervisor_config.supervisee_contracts):
            self.assertEqual(
                supervisee_contract.alias, expected_supervisee_contracts[i].contract_id
            )
            self.assertEqual(
                supervisee_contract.contract_file_path,
                expected_supervisee_contracts[i].contract_file,
            )
            self.assertDictEqual(
                supervisee_contract.template_params,
                expected_supervisee_contracts[i].template_parameters,
            )
            self.assertEqual(
                supervisee_contract.smart_contract_version_id,
                expected_supervisee_contracts[i].version,
            )
            self.assertEqual(len(supervisee_contract.account_configs), 1)

            supervisee_contract_account = supervisee_contract.account_configs[0]
            self.assertEqual(
                supervisee_contract_account.account_id_base,
                f"{expected_supervisee_contracts[i].account_name} ",
            )
            self.assertDictEqual(
                supervisee_contract_account.instance_params,
                expected_supervisee_contracts[i].instance_parameters,
            )
            self.assertEqual(
                supervisee_contract_account.number_of_accounts,
                expected_supervisee_contracts[i].instances,
            )

    def test_get_plan_logs(self):
        plan_logs_main = simulation_test_utils.get_plan_logs(
            res=self.supervisor_res, plan_id="1"
        )

        self.assertIn('plan "1"', plan_logs_main)
        self.assertIn('created plan "1"', plan_logs_main)
        self.assertIn(
            'created account plan association for account "Checking Account"',
            plan_logs_main,
        )
        self.assertNotIn('created account "Checking Account"', plan_logs_main)

    def test_get_plan_created(self):
        plan_created = simulation_test_utils.get_plan_created(
            res=self.supervisor_res, plan_id="1"
        )

        self.assertTrue(plan_created)

        plan_not_existing_id = simulation_test_utils.get_plan_created(
            res=self.supervisor_res, plan_id="1", supervisor_version_id="5"
        )

        self.assertFalse(plan_not_existing_id)

    def test_get_plan_assoc_created(self):
        checking_account_plan_created = simulation_test_utils.get_plan_assoc_created(
            res=self.supervisor_res, plan_id="1", account_id="Checking Account"
        )

        self.assertTrue(checking_account_plan_created)

        savings_account_plan_created = simulation_test_utils.get_plan_assoc_created(
            res=self.supervisor_res, plan_id="1", account_id="Savings Account"
        )

        self.assertTrue(savings_account_plan_created)

        plan_not_created = simulation_test_utils.get_plan_assoc_created(
            res=self.supervisor_res, plan_id="1", account_id="Youth Account"
        )

        self.assertFalse(plan_not_created)

    def test_get_logs_with_timestamp(self):
        all_logs = simulation_test_utils.get_logs_with_timestamp(res=self.sample_res)
        timestamp_1 = datetime(year=2019, month=1, day=1, tzinfo=timezone.utc)
        timestamp_2 = datetime(year=2019, month=1, day=21, tzinfo=timezone.utc)

        self.assertIn("created flag definition", "".join(all_logs[timestamp_1]))
        self.assertNotIn("transactions must be in GBP", "".join(all_logs[timestamp_1]))
        self.assertIn("transactions must be in GBP", "".join(all_logs[timestamp_2]))

    def test_get_default_postings(self):
        postings = simulation_test_utils.get_postings(res=self.sample_res)

        first_posting = postings[0]
        last_posting = postings[-1]

        self.assertEqual(first_posting["account_id"], "Main account")
        self.assertEqual(first_posting["account_address"], "DEFAULT")

        self.assertEqual(last_posting["account_id"], "Main account")
        self.assertEqual(last_posting["account_address"], "DEFAULT")

        postings_supervisor = simulation_test_utils.get_postings(
            res=self.supervisor_res, account_id="Savings Account"
        )

        first_posting = postings_supervisor[0]
        last_posting = postings_supervisor[-1]

        self.assertEqual(first_posting["account_id"], "Savings Account")
        self.assertEqual(first_posting["account_address"], "DEFAULT")

        self.assertEqual(last_posting["account_id"], "Savings Account")
        self.assertEqual(last_posting["account_address"], "DEFAULT")

    def test_get_posting_instruction_batch_at_with_one_result(self):
        account_id = "Main account"
        pibs = simulation_test_utils.get_posting_instruction_batch(
            res=self.sample_res, event_type="outbound_hard_settlement"
        )

        self.assertListEqual(
            pibs[account_id].at(datetime(2019, 1, 1, tzinfo=timezone.utc)),
            [
                {
                    "amount": "110",
                    "denomination": "GBP",
                    "target_account": {"account_id": "Main account"},
                    "internal_account_id": "1",
                    "advice": False,
                    "target_account_id": "Main account",
                }
            ],
        )

    def test_get_posting_instruction_batch_at_with_multiple_results(self):
        account_id = "Easy Access Saver Account"
        pibs = simulation_test_utils.get_posting_instruction_batch(
            res=self.sample_res, event_type="release"
        )

        self.assertListEqual(
            pibs[account_id].at(datetime(2019, 3, 3, tzinfo=timezone.utc)),
            [
                {
                    "amount": "100",
                    "denomination": "GBP",
                    "target_account_id": "Easy Access Saver Account",
                    "internal_account_id": "DUMMY_DEPOSITING_ACCOUNT",
                },
                {
                    "amount": "50",
                    "denomination": "GBP",
                    "target_account_id": "Easy Access Saver Account",
                    "internal_account_id": "DUMMY_DEPOSITING_ACCOUNT",
                },
            ],
        )

    def test_get_posting_instruction_batch_latest(self):
        account_id = "Easy Access Saver Account"
        pibs = simulation_test_utils.get_posting_instruction_batch(
            res=self.sample_res, event_type="release"
        )

        self.assertListEqual(
            pibs[account_id].latest(),
            [
                {
                    "amount": "80",
                    "denomination": "GBP",
                    "target_account_id": "Easy Access Saver Account",
                    "internal_account_id": "DUMMY_DEPOSITING_ACCOUNT",
                }
            ],
        )

    def test_get_posting_instruction_batch_all(self):
        account_id = "Easy Access Saver Account"
        pibs = simulation_test_utils.get_posting_instruction_batch(
            res=self.sample_res, event_type="release"
        )

        self.assertEqual(len(pibs[account_id].all()), 2)

    def test_get_posting_instruction_batch_before(self):
        account_id = "Easy Access Saver Account"
        pibs = simulation_test_utils.get_posting_instruction_batch(
            res=self.sample_res, event_type="release"
        )

        self.assertEqual(
            len(pibs[account_id].before(datetime(2019, 3, 4, tzinfo=timezone.utc))), 2
        )

    def test_get_posting_instruction_batch_with_no_results(self):
        pibs = simulation_test_utils.get_posting_instruction_batch(
            res=self.sample_res, event_type="INVALID"
        )

        self.assertEqual(len(pibs), 0)

    def test_get_num_default_postings(self):
        num_postings = simulation_test_utils.get_num_postings(
            res=self.sample_res, balance_dimensions=BalanceDimensions(address="DEFAULT")
        )
        self.assertEqual(num_postings, 4)

    def test_get_processed_scheduled_events(self):
        scheduled_events_timestamp = (
            simulation_test_utils.get_processed_scheduled_events(
                res=self.sample_res, event_id="ACCRUE_INTEREST", account_id="1"
            )
        )

        self.assertEqual("2019-01-01T00:00:00Z", scheduled_events_timestamp[0])
        self.assertEqual("2019-01-02T00:00:00Z", scheduled_events_timestamp[1])
        self.assertEqual("2019-03-02T00:00:00Z", scheduled_events_timestamp[-1])

        supervised_events_timestamp = (
            simulation_test_utils.get_processed_scheduled_events(
                res=self.supervisor_res,
                event_id="PUBLISH_COMBINED_EXTRACT",
                plan_id="1",
            )
        )

        self.assertEqual("2020-05-12T23:59:00Z", supervised_events_timestamp[0])
        self.assertEqual("2020-05-13T23:59:00Z", supervised_events_timestamp[1])
        self.assertEqual("2020-06-30T23:59:00Z", supervised_events_timestamp[-1])

    def test_get_instantiated_workflows(self):
        workflow_list = simulation_test_utils.get_instantiated_workflows(
            res=self.sample_res
        )

        for _, workflows in workflow_list:
            for workflow in workflows:
                self.assertTrue(workflow["workflow_definition_id"])

        accrual_date = datetime(2019, 2, 5, tzinfo=timezone.utc)
        self.assertIn(
            "MOCK_INTEREST_ACCRUAL_WORKFLOW",
            workflow_list.at(accrual_date)[0]["workflow_definition_id"],
        )

        overdraft_date_1 = datetime(2019, 1, 1, tzinfo=timezone.utc)
        self.assertIn(
            "MOCK_FULL_CONTRACT_OVERDRAFT_FEE_NOTIFICATION",
            workflow_list.at(overdraft_date_1)[0]["workflow_definition_id"],
        )

        overdraft_date_2 = datetime(2019, 1, 2, tzinfo=timezone.utc)
        self.assertNotIn(
            "MOCK_FULL_CONTRACT_OVERDRAFT_FEE_NOTIFICATION",
            workflow_list.at(overdraft_date_2)[0]["workflow_definition_id"],
        )

    def test_get_workflows_by_id(self):
        workflow_fee = simulation_test_utils.get_workflows_by_id(
            res=self.sample_res,
            workflow_definition_id="MOCK_FULL_CONTRACT_OVERDRAFT_FEE_NOTIFICATION",
        ).latest()
        self.assertEquals(
            "MOCK_FULL_CONTRACT_OVERDRAFT_FEE_NOTIFICATION",
            workflow_fee["workflow_definition_id"],
        )

        workflow_accrual = simulation_test_utils.get_workflows_by_id(
            res=self.sample_res, workflow_definition_id="MOCK_INTEREST_ACCRUAL_WORKFLOW"
        ).latest()
        self.assertEquals(
            "MOCK_INTEREST_ACCRUAL_WORKFLOW", workflow_accrual["workflow_definition_id"]
        )

    def test_print_json(self):
        output = sys_stdout(simulation_test_utils.print_json, "debug", self.sample_res)

        self.assertIn("debug", output)
        self.assertIn("account_notes", output)
        self.assertIn("balances", output)

    def test_print_postings(self):
        output = sys_stdout(simulation_test_utils.print_postings, res=self.sample_res)

        self.assertIn("account_address", output)
        self.assertNotIn("balances", output)

    def test_print_account_notes(self):
        output = sys_stdout(
            simulation_test_utils.print_account_notes,
            res=self.sample_res,
            account_id="1",
        )
        self.assertIn("Account note", output)
        self.assertNotIn("logs", output)

    def test_print_log(self):
        output = sys_stdout(simulation_test_utils.print_log, res=self.sample_res)
        self.assertIn("flag", output)
        self.assertNotIn("1030", output)

    @patch("logging.Logger.warning")
    def test_compile_chrono_events_warnings(self, mock):
        start = datetime(year=2021, month=1, day=1, tzinfo=timezone.utc)
        end = start + timedelta(days=1, hours=8)

        setup_event = create_account_instruction(
            timestamp=start + timedelta(minutes=30)
        )

        subtest_1_event = create_inbound_hard_settlement_instruction(
            "1500.00", start + timedelta(hours=1), denomination="USD"
        )
        subtest_2_event = create_outbound_hard_settlement_instruction(
            "5.00",
            start + timedelta(minutes=1),
            denomination="USD",
            instruction_details={"transaction_code": "6011"},
        )
        subtest_6_event = create_outbound_hard_settlement_instruction(
            "5.00",
            start + timedelta(days=10),
            denomination="USD",
            instruction_details={"transaction_code": "6011"},
        )

        sub_tests = [
            SubTest(
                description="1st sub test",
                events=[subtest_1_event],
                expected_balances_at_ts={
                    start
                    + timedelta(hours=3, minutes=1): {
                        "Main account": [
                            (BalanceDimensions(denomination="USD"), "600.00")
                        ]
                    }
                },
                expected_derived_parameters=[
                    ExpectedDerivedParameter(
                        timestamp=start,
                        account_id="Main account",
                        name="some_name",
                        value="some_value",
                    ),
                ],
            ),
            SubTest(
                description="2nd sub test",
                events=[subtest_2_event],
                expected_balances_at_ts={
                    start
                    + timedelta(hours=1, minutes=1): {
                        "Main account": [
                            (BalanceDimensions(denomination="USD"), "600.00")
                        ]
                    }
                },
            ),
            SubTest(
                description="3rd sub test",
                expected_posting_rejections=[
                    ExpectedRejection(
                        start + timedelta(hours=1),
                        rejection_type="InsufficientFunds",
                        rejection_reason="some reason",
                    )
                ],
            ),
            SubTest(
                description="4th sub test",
                expected_schedules=[
                    ExpectedSchedule(
                        run_times=[
                            start + timedelta(hours=2),
                            start + timedelta(minutes=30),
                        ],
                        event_id="big event",
                        account_id="big account",
                    )
                ],
            ),
            SubTest(
                description="5th sub test",
                expected_workflows=[
                    ExpectedWorkflow(
                        run_times=[start + timedelta(hours=1, minutes=45)],
                        workflow_definition_id="hard_working_flow",
                        count=1,
                    )
                ],
            ),
            SubTest(
                description="6th sub test",
                events=[subtest_6_event],
                expected_balances_at_ts={
                    start
                    + timedelta(hours=1, minutes=30): {
                        "Main account": [
                            (BalanceDimensions(denomination="USD"), "600.00")
                        ]
                    }
                },
                expected_derived_parameters=[
                    ExpectedDerivedParameter(
                        timestamp=start + timedelta(hours=1, minutes=30),
                        account_id="Another account",
                        name="some_name",
                        value="some_value",
                    ),
                ],
            ),
            SubTest(
                description="7th sub test",
                expected_balances_at_ts={
                    start
                    + timedelta(hours=4): {
                        "Main account": [
                            (BalanceDimensions(denomination="USD"), "600.00")
                        ]
                    }
                },
            ),
        ]

        test_scenario = SimulationTestScenario(
            start=start, end=end, sub_tests=sub_tests
        )

        (
            compiled_events,
            derived_param_outputs,
        ) = simulation_test_utils.compile_chrono_events(test_scenario, [setup_event])
        mock.assert_has_calls(
            [
                call(
                    'Subtest "2nd sub test" contains event timestamp before the previous one.'
                ),
                call(
                    'Subtest "2nd sub test" contains assertion timestamp before the previous one.'
                ),
                call(
                    'Subtest "3rd sub test" contains assertion timestamp before the previous one.'
                ),
                call(
                    'Subtest "4th sub test" contains assertion timestamp before the previous one.'
                ),
                call(
                    'Subtest "5th sub test" contains assertion timestamp before the previous one.'
                ),
                call(
                    'Subtest "6th sub test" contains assertion timestamp before the previous one.'
                ),
                call("last assertion or event happens outside of simulation window"),
            ]
        )

        self.assertEqual(
            compiled_events,
            [setup_event, subtest_1_event, subtest_2_event, subtest_6_event],
        )

        self.assertEqual(
            derived_param_outputs,
            [
                ("Main account", start),
                ("Another account", start + timedelta(hours=1, minutes=30)),
            ],
        )

    def test_compile_chrono_events_exceptions(self):
        start = datetime(year=2021, month=1, day=1, tzinfo=timezone.utc)
        end = start + timedelta(days=1, hours=8)

        setup_events = [
            create_account_instruction(timestamp=start + timedelta(hours=2))
        ]

        sub_tests = [
            SubTest(
                description="1st sub test",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "1500.00", start + timedelta(hours=1), denomination="USD"
                    )
                ],
                expected_balances_at_ts={
                    start
                    + timedelta(hours=3, minutes=1): {
                        "Main account": [
                            (BalanceDimensions(denomination="USD"), "600.00")
                        ]
                    }
                },
            ),
        ]

        test_scenario = SimulationTestScenario(
            start=start, end=end, sub_tests=sub_tests
        )

        with self.assertRaises(ValueError) as ex:
            simulation_test_utils.compile_chrono_events(test_scenario, setup_events)

        self.assertIn(
            f"First custom event at {start + timedelta(hours=1)}, it needs to be after "
            f"{start + timedelta(hours=2)}, when account and plan setup events are complete",
            str(ex.exception),
        )


def sys_stdout(func, *args, **kwargs):
    with patch("sys.stdout", new=io.StringIO()) as sys_out:
        func(*args, **kwargs)
        return sys_out.getvalue()
