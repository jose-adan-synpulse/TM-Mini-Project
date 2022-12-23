# Copyright @ 2020-2022 Thought Machine Group Limited. All rights reserved.
# standard libs
import sys
import unittest
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from json import dumps
from typing import Dict, List
from unittest import skip

# common
from common.test_utils.common.balance_helpers import BalanceDimensions
from common.test_utils.postings.posting_classes import (
    InboundHardSettlement,
    OutboundHardSettlement,
)
from common.test_utils.contracts.simulation.simulation_test_utils import (
    SimulationTestCase,
    get_balances,
    get_logs,
    get_num_postings,
    get_processed_scheduled_events,
)
from common.test_utils.contracts.simulation.data_objects.data_objects import (
    SimulationEvent,
    AccountConfig,
    ContractConfig,
    ContractModuleConfig,
    ExpectedDerivedParameter,
    ExpectedRejection,
    ExpectedWorkflow,
    SimulationTestScenario,
    SubTest,
)
from common.test_utils.contracts.simulation.helper import (
    create_account_instruction,
    create_posting_instruction_batch,
    create_auth_adjustment_instruction,
    create_flag_definition_event,
    create_flag_event,
    create_inbound_authorisation_instruction,
    create_inbound_hard_settlement_instruction,
    create_outbound_authorisation_instruction,
    create_outbound_hard_settlement_instruction,
    create_settlement_event,
    create_transfer_instruction,
    update_account_status_pending_closure,
)

CONTRACT_FILE = "casa/contracts/casa.py"
CONTRACT_FILES = [CONTRACT_FILE]
DEFAULT_DIMENSIONS = BalanceDimensions()

ACCRUED_INCOMING_INTEREST_DIMENSIONS = BalanceDimensions(address="ACCRUED_INCOMING_INTEREST")
ACCRUED_INTEREST_DIMENSIONS = BalanceDimensions(address="ACCRUED_INTEREST")
MONTHLY_MAINTENANCE_FEE_DIMENSIONS = BalanceDimensions(address="MONTHLY_MAINTENANCE_FEES")

INTERNAL_ACCOUNT = "Internal account"
MAIN_ACCOUNT = "Main account"
CASA_CONTRACT_VERSION_ID = "1000"

default_simulation_start_date = datetime(year=2022, month=1, day=1, tzinfo=timezone.utc)

default_template_params = {
    'denomination': 'PHP',
    'fee_tiers': '{ "tier1": "0.135", '
                    '"tier2": "0.098", '
                    '"tier3": "0.045", '
                    '"tier4": "0.035", '
                    '"tier5": "0.03"}',
    'fee_tier_ranges': '{ "tier1": {"min": 1000, "max": 2999},'
                        '"tier2": {"min": 3000, "max": 4999},'
                        '"tier3": {"min": 5000, "max": 7499},'
                        '"tier4": {"min": 7500, "max": 14999},'
                        '"tier5": {"min": 15000, "max": 20000}}',
    'internal_account': 'Internal account',
    'base_interest_rate': '.002', 
    'bonus_interest_rate': '.005', 
    'flat_fee': '50',
    'bonus_interest_amount_threshold': '5000',
    'minimum_balance_maintenance_fee_waive': '150'
}

default_instance_params = {
    
}


class CASATest(SimulationTestCase):
    @classmethod
    def setupClass(cls):
        cls.contract_filepaths = CONTRACT_FILES
        super().setupClass()

    def default_create_account_instruction(self, start, instance_param_vals=None):
        return create_account_instruction(
            timestamp=start,
            account_id=MAIN_ACCOUNT,
            product_id=CASA_CONTRACT_VERSION_ID,
            instance_param_vals=instance_param_vals or default_instance_params,
        )

    def run_test(
        self,
        start: datetime,
        end: datetime,
        events: List,
        template_parameters: Dict[str, str] = None,
    ):

        contract_config = ContractConfig(
            contract_file_path=CONTRACT_FILE,
            template_params=default_template_params,
            account_configs=[
                AccountConfig(
                    instance_params=default_instance_params,
                    account_id_base=MAIN_ACCOUNT,
                )
            ],
            smart_contract_version_id=CASA_CONTRACT_VERSION_ID,
        )

        return self.client.simulate_smart_contract(
            contract_codes=self.smart_contract_contents.copy(),
            smart_contract_version_ids=[CASA_CONTRACT_VERSION_ID],
            start_timestamp=start,
            end_timestamp=end,
            templates_parameters=[
                template_parameters or default_template_params,
            ],
            internal_account_ids=[INTERNAL_ACCOUNT],
            events=events,
            contract_config=contract_config,
        )

    def _get_simulation_test_scenario(
        self,
        start,
        end,
        sub_tests,
        template_params=None,
        instance_params=None,
        internal_accounts=None,
    ):
        contract_config = ContractConfig(
            contract_file_path=CONTRACT_FILE,
            template_params=template_params or default_template_params,
            smart_contract_version_id=CASA_CONTRACT_VERSION_ID,
            account_configs=[
                AccountConfig(
                    instance_params=instance_params or default_instance_params,
                )
            ]
        )
        return SimulationTestScenario(
            start=start,
            end=end,
            sub_tests=sub_tests,
            contract_config=contract_config,
            internal_accounts=internal_accounts or [INTERNAL_ACCOUNT],
        )

    def test_deposits_denominations(self):
        start = default_simulation_start_date
        end = start + relativedelta(days=1)

        sub_tests = [
            SubTest(
                description="test balance correct after single deposit made to account",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "283.45", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(hours=1, seconds=1): {
                        "Main account": [(DEFAULT_DIMENSIONS, "283.45")]
                    },
                },
            ),
            SubTest(
                description="test other denomination other than PHP inbound payments",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "100", start + timedelta(hours=5), denomination="EUR",  target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT
                    ),
                    create_inbound_hard_settlement_instruction(
                        "100", start + timedelta(hours=5), denomination="GBP",  target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT
                    )
                ],
                expected_posting_rejections=[
                    ExpectedRejection(
                        timestamp=start + timedelta(hours=5),
                        account_id="Main account",
                        rejection_type="WrongDenomination",
                        rejection_reason="Cannot make transaction in given denominations; "
                        "transactions must be in PHP",
                    )
                ]
            )
        ]
        
        test_scenario = self._get_simulation_test_scenario(
            start=start,
            end=end,
            sub_tests=sub_tests,
        )
        self.run_test_scenario(test_scenario)

    def test_interest_accrual_application(self):
        start = default_simulation_start_date
        end = start + relativedelta(days=6, hours=3)

        sub_tests = [
            SubTest(
                description="test base interest accrual after 2 days",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "1500", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=2, hours=1, seconds=1): {
                        MAIN_ACCOUNT : [(DEFAULT_DIMENSIONS, "1506.006")]
                    },
                },
            ),
            SubTest(
                description="test bonus interest accrual at 4th day",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "3500", start + relativedelta(days=3, hours=9), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=4, hours=5, seconds=1): {
                        MAIN_ACCOUNT : [(DEFAULT_DIMENSIONS, "5044.0811")]
                    },
                },
            ),
            SubTest(
                description="test base interest accrual after withdrawal",
                events=[
                    create_outbound_hard_settlement_instruction(
                        "3500", start + relativedelta(days=5, hours=2), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=6, hours=2, seconds=1): {
                        MAIN_ACCOUNT : [(DEFAULT_DIMENSIONS, "1582.5485")]
                    },
                },
            )
        ]

        test_scenario = self._get_simulation_test_scenario(
            start=start,
            end=end,
            sub_tests=sub_tests,

        )
        self.run_test_scenario(test_scenario)

    def test_flat_maintenance_fee(self):
        start = default_simulation_start_date
        end = start + relativedelta(months=2, hours=2)

        sub_tests = [
            SubTest(
                description="test flat fee after 1 month",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "1500", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=31, hours=1, seconds=11): {
                        MAIN_ACCOUNT : [(MONTHLY_MAINTENANCE_FEE_DIMENSIONS, "-50")]
                    },
                },
            ),
            SubTest(
                description="test flat fee waived due to high monthly mean balance (3rd month)",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "4000", start + relativedelta(months=1, day=2), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(months=2, day=1, seconds=1): {
                        MAIN_ACCOUNT : [(MONTHLY_MAINTENANCE_FEE_DIMENSIONS, "-50")]
                    },
                },
            ),
        ]

        test_scenario = self._get_simulation_test_scenario(
            start=start,
            end=end,
            sub_tests=sub_tests,
        )
        self.run_test_scenario(test_scenario)

    def test_tier_maintenance_fee(self):
        test_template_params = {
            'denomination': 'PHP',
            'fee_tiers': '{ "tier1": "0.135", '
                            '"tier2": "0.098", '
                            '"tier3": "0.045", '
                            '"tier4": "0.035", '
                            '"tier5": "0.03"}',
            'fee_tier_ranges': '{ "tier1": {"min": 1000, "max": 2999},'
                                '"tier2": {"min": 3000, "max": 4999},'
                                '"tier3": {"min": 5000, "max": 7499},'
                                '"tier4": {"min": 7500, "max": 14999},'
                                '"tier5": {"min": 15000, "max": 20000}}',
            'internal_account': 'Internal account',
            'base_interest_rate': '.002', 
            'bonus_interest_rate': '.005', 
            'flat_fee': '0',
            'bonus_interest_amount_threshold': '5000',
            'minimum_balance_maintenance_fee_waive': '150'
        }

        start = default_simulation_start_date
        end = start + relativedelta(months=1, hours=2)

        sub_tests = [
            SubTest(
                description="test tier 1 maintenance fee after 1 month",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "1500", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=31, hours=0, seconds=0): {
                        MAIN_ACCOUNT : [(MONTHLY_MAINTENANCE_FEE_DIMENSIONS, "-0.135")]
                    },
                },
            ),
            SubTest(
                description="test tier 2 maintenance fee after 1 month",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "3000", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=31, hours=0, seconds=0): {
                        MAIN_ACCOUNT : [(MONTHLY_MAINTENANCE_FEE_DIMENSIONS, "-0.098")]
                    },
                },
            ),
            SubTest(
                description="test tier 3 maintenance fee after 1 month",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "5000", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=31, hours=0, seconds=0): {
                        MAIN_ACCOUNT : [(MONTHLY_MAINTENANCE_FEE_DIMENSIONS, "-0.045")]
                    },
                },
            ),
            SubTest(
                description="test tier 4 maintenance fee after 1 month",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "7500", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=31, hours=0, seconds=0): {
                        MAIN_ACCOUNT : [(MONTHLY_MAINTENANCE_FEE_DIMENSIONS, "-0.035")]
                    },
                },
            ),
            SubTest(
                description="test tier 5 maintenance fee after 1 month",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "15000", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=31, hours=0, seconds=0): {
                        MAIN_ACCOUNT : [(MONTHLY_MAINTENANCE_FEE_DIMENSIONS, "-0.03")]
                    },
                },
            ),
            SubTest(
                description="test balance out of tier bounds fee waived",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "19000", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(days=31, hours=0, seconds=0): {
                        MAIN_ACCOUNT : [(MONTHLY_MAINTENANCE_FEE_DIMENSIONS, "0")]
                    },
                },
            ),
        ]

        for sub_test in sub_tests: 
            test_scenario = self._get_simulation_test_scenario(
                start=start,
                end=end,
                sub_tests=[sub_test],
                instance_params=test_template_params,
                template_params=test_default_
            )
            self.run_test_scenario(test_scenario)

    def test_withdraw_denominations(self):
        start = default_simulation_start_date
        end = start + relativedelta(days=1)

        sub_tests = [
            SubTest(
                description="test balance correct after single withdraw made to account",
                events=[
                    create_inbound_hard_settlement_instruction(
                        "5000", start + relativedelta(hours=1), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                    create_outbound_hard_settlement_instruction(
                        "3500", start + relativedelta(hours=3), target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT, denomination="PHP"
                    ),
                ],
                expected_balances_at_ts={
                    start
                    + relativedelta(hours=3, seconds=60): {
                        "Main account": [(DEFAULT_DIMENSIONS, "1500")]
                    },
                },
            ),
            SubTest(
                description="test other denomination other than PHP outbound payments",
                events=[
                    create_outbound_hard_settlement_instruction(
                        "100", start + timedelta(hours=5), denomination="EUR",  target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT
                    ),
                    create_outbound_hard_settlement_instruction(
                        "100", start + timedelta(hours=5), denomination="GBP",  target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT
                    )
                ],
                expected_posting_rejections=[
                    ExpectedRejection(
                        timestamp=start + timedelta(hours=5),
                        account_id="Main account",
                        rejection_type="WrongDenomination",
                        rejection_reason="Cannot make transaction in given denominations; "
                        "transactions must be in PHP",
                    )
                ]
            ),
            SubTest(
                description="test withdraw amount greater than current balance",
                events=[                                           
                    create_outbound_hard_settlement_instruction(
                        "4000", start + timedelta(hours=6), denomination="PHP",  target_account_id = MAIN_ACCOUNT, internal_account_id = INTERNAL_ACCOUNT
                    ),
                ],
                expected_posting_rejections=[
                    ExpectedRejection(
                        timestamp=start + timedelta(hours=6),
                        account_id="Main account",
                        rejection_type="InsufficientFunds",
                        rejection_reason="Cannot withdraw proposed amount."
                    )
                ]
            )
        ]
        
        test_scenario = self._get_simulation_test_scenario(
            start=start,
            end=end,
            sub_tests=sub_tests,
        )
        self.run_test_scenario(test_scenario)