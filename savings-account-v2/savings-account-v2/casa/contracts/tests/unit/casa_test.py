import vault_caller
from datetime import datetime, timezone
from decimal import Decimal
import os
import unittest
import product_test_utils
import json

core_api_url = "https://core-api-demo.sparrow.tmachine.io/"
auth_token = "A0002591307220302710456!avhtt8iRTQtTu1oh1GQhTpsgleteAEnzvICy3vpGv3Ouwxt2LuxUQrN/A2IFx7RCiyqn4Gsk1W1hFi6u/6pTP6ve+9U="
CONTRACT_FILE = 'C:/Users/jose.adan/OneDrive - Synpulse/Documents/Thought Machine/TM mini proj/savings-account-v2/savings-account-v2/casa/contracts/casa.py'

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
    'internal_account': 'internal_account'
}
default_instance_params = {
    'base_interest_rate': '.002',
    'bonus_interest_rate': '.005',
    'bonus_interest_amount_threshold': '3000',
    'minimum_balance_maintenance_fee_waive': '500',
    'flat_fee': '25',
}

class TutorialTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        contract = os.path.join(os.path.dirname(__file__), CONTRACT_FILE)
        if not core_api_url or not auth_token:
            raise ValueError("Please provide values for core_api_url and auth_token")
        with open(contract) as smart_contract_file:
            self.smart_contract_contents = smart_contract_file.read()
        self.client = vault_caller.Client(
            core_api_url=core_api_url,
            auth_token=auth_token,
        )

    def make_simulate_contracts_call(
        self,
        start,
        end,
        template_params,
        instance_params,
        instructions=[],
    ):
        return self.client.simulate_contracts(
            start_timestamp=start,
            end_timestamp=end,
            smart_contracts=[
                {
                    "smart_contract_version_id": "1",
                    "code": self.smart_contract_contents,
                    "smart_contract_param_vals": template_params,
                },
                {
                    "smart_contract_version_id": "2",
                    "code": "api = '3.6.0'",
                },
            ],
            instructions=[
                # Main account
                vault_caller.SimulationInstruction(
                    start,
                    {
                        "create_account": {
                            "id": "main_account",
                            "product_version_id": "1",
                            "instance_param_vals": instance_params,
                        }
                    },
                ),
                # Our internal account.
                vault_caller.SimulationInstruction(
                    start,
                    {
                        "create_account": {
                            "id": "internal_account",
                            "product_version_id": "2",
                        }
                    },
                ),
            ]
            + instructions,
        )

    def test_daily_accrue_base_interest(self):
        start = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        end = datetime(year=2019, month=1, day=4, hour=9, tzinfo=timezone.utc)
        test_instance_params = {
            'base_interest_rate': '.002',
            'bonus_interest_rate': '.005',
            'bonus_interest_amount_threshold': '5000',
            'minimum_balance_maintenance_fee_waive': '5000',
            'flat_fee': '250',
        }
        instructions = [] 

        deposit_instruction = product_test_utils.create_deposit_instruction(
            amount="3500", timestamp=start.isoformat()
        )

        instructions.append(vault_caller.SimulationInstruction(start, deposit_instruction))

        res = self.make_simulate_contracts_call(
            start,
            end,
            default_template_params,
            test_instance_params,
            instructions,
        )

        final_balances = product_test_utils.get_final_balances(res[5]["result"]["balances"]["main_account"]["balances"])
        final_balances_2 = product_test_utils.get_final_balances(res[9]["result"]["balances"]["main_account"]["balances"])
        final_balances_3 = product_test_utils.get_final_balances(res[13]["result"]["balances"]["main_account"]["balances"])
        
        self.assertEqual("7", final_balances["ACCRUED_INCOMING_INTEREST"])
        self.assertEqual("7.014", final_balances_2["ACCRUED_INCOMING_INTEREST"])
        self.assertEqual("7.028", final_balances_3["ACCRUED_INCOMING_INTEREST"])

    def test(self):
        start = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        instruction2 = datetime(year=2019, month=1, day=3, hour=9, tzinfo=timezone.utc)
        end = datetime(year=2019, month=1, day=5, hour=9, tzinfo=timezone.utc)
        test_instance_params = {
            'base_interest_rate': '.002',
            'bonus_interest_rate': '.005',
            'bonus_interest_amount_threshold': '5000',
            'minimum_balance_maintenance_fee_waive': '5000',
            'flat_fee': '250',
        }
        instructions = [] 

        deposit_instruction = product_test_utils.create_deposit_instruction(
            amount="1500", timestamp=start.isoformat()
        )

        instructions.append(vault_caller.SimulationInstruction(start, deposit_instruction))

        deposit_instruction2 = product_test_utils.create_deposit_instruction(
            amount="3500", timestamp=instruction2.isoformat(), client_transaction_id="2"
        )

        instructions.append(vault_caller.SimulationInstruction(instruction2, deposit_instruction2))

        res = self.make_simulate_contracts_call(
            start,
            end,
            default_template_params,
            test_instance_params,
            instructions,
        )

        final_balances = product_test_utils.get_final_balances(res[5]["result"]["balances"]["main_account"]["balances"])
        final_balances_2 = product_test_utils.get_final_balances(res[9]["result"]["balances"]["main_account"]["balances"])
        final_balances_3 = product_test_utils.get_final_balances(res[13]["result"]["balances"]["main_account"]["balances"])
        
        self.assertEqual("7", final_balances["ACCRUED_INCOMING_INTEREST"])
        self.assertEqual("7.014", final_balances_2["ACCRUED_INCOMING_INTEREST"])
        self.assertEqual("7.028", final_balances_3["ACCRUED_INCOMING_INTEREST"])

    def test_daily_accrue_base_interest_apply_interest(self):
        start = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        end = datetime(year=2019, month=1, day=4, hour=9, tzinfo=timezone.utc)
        test_instance_params = {
            'base_interest_rate': '.002',
            'bonus_interest_rate': '.005',
            'bonus_interest_amount_threshold': '5000',
            'minimum_balance_maintenance_fee_waive': '5000',
            'flat_fee': '250',
        }
        instructions = [] 

        deposit_instruction = product_test_utils.create_deposit_instruction(
            amount="3500", timestamp=start.isoformat(), client_transaction_id="1"
        )
        instructions.append(vault_caller.SimulationInstruction(start, deposit_instruction))

        res = self.make_simulate_contracts_call(
            start,
            end,
            default_template_params,
            test_instance_params,
            instructions,
        )
        
        final_balances = product_test_utils.get_final_balances(res[7]["result"]["balances"]["main_account"]["balances"])
        final_balances_2 = product_test_utils.get_final_balances(res[11]["result"]["balances"]["main_account"]["balances"])
        final_balances_3 = product_test_utils.get_final_balances(res[15]["result"]["balances"]["main_account"]["balances"])

        self.assertEqual("7", final_balances["ACCRUED_INTEREST"])
        self.assertEqual("14.014", final_balances_2["ACCRUED_INTEREST"])
        self.assertEqual("21.042", final_balances_3["ACCRUED_INTEREST"])

    def test_monthly_maintenance_flat_fee_application(self):
        start = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        instruction1 = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        end = datetime(year=2019, month=2, day=2, hour=10, tzinfo=timezone.utc)
        instructions = []

        deposit_instruction = product_test_utils.create_deposit_instruction(
            amount="3000", timestamp=instruction1.isoformat(), client_transaction_id="1"
        )
        instructions.append(vault_caller.SimulationInstruction(instruction1, deposit_instruction))

        res = self.make_simulate_contracts_call(
            start,
            end,
            default_template_params,
            default_instance_params,
            instructions,
        )
        
        final_balances = product_test_utils.get_final_balances(res[131]["result"]["balances"]["main_account"]["balances"])
        self.assertEqual("-25", final_balances["MONTHLY_MAINTENANCE_FEES"])

    def test_tiered_monthly_maintenance_fee_application(self):
        start = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        instruction1 = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        end = datetime(year=2019, month=2, day=2, hour=9, tzinfo=timezone.utc)
        instructions = []

        test_template_params = {
            'denomination': 'PHP',
            'fee_tiers': '{ "tier1": "135", '
                            '"tier2": "98", '
                            '"tier3": "45", '
                            '"tier4": "35", '
                            '"tier5": "3"}',
            'fee_tier_ranges': '{ "tier1": {"min": 1000, "max": 2999},'
                                '"tier2": {"min": 3000, "max": 4999},'
                                '"tier3": {"min": 5000, "max": 7499},'
                                '"tier4": {"min": 7500, "max": 14999},'
                                '"tier5": {"min": 15000, "max": 20000}}',
            'internal_account': 'internal_account'
        }

        test_instance_params = {
            'base_interest_rate': '.002',
            'bonus_interest_rate': '.005',
            'bonus_interest_amount_threshold': '3000',
            'minimum_balance_maintenance_fee_waive': '10000',
            'flat_fee': '0'
        }


        deposit_instruction = product_test_utils.create_deposit_instruction(
            amount="4975", timestamp=instruction1.isoformat(), client_transaction_id="1"
        )
        instructions.append(vault_caller.SimulationInstruction(instruction1, deposit_instruction))

        res = self.make_simulate_contracts_call(
            start,
            end,
            test_template_params,
            test_instance_params,
            instructions,
        )
        
        final_balances = product_test_utils.get_final_balances(res[131]["result"]["balances"]["main_account"]["balances"])
        self.assertEqual("-45", final_balances["MONTHLY_MAINTENANCE_FEES"])

    def test_bonus_interest_accruals(self):
        start = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        end = datetime(year=2019, month=1, day=10, hour=9, tzinfo=timezone.utc)
        test_instance_params = {
            'base_interest_rate': '.002',
            'bonus_interest_rate': '.005',
            'bonus_interest_amount_threshold': '2500',
            'minimum_balance_maintenance_fee_waive': '5000',
            'flat_fee': '0',
        }
        instructions = [] 

        deposit_instruction = product_test_utils.create_deposit_instruction(
            amount="2475", timestamp=start.isoformat()
        )

        instructions.append(vault_caller.SimulationInstruction(start, deposit_instruction))

        res = self.make_simulate_contracts_call(
            start,
            end,
            default_template_params,
            test_instance_params,
            instructions,
        )

        final_balances = product_test_utils.get_final_balances(res[25]["result"]["balances"]["main_account"]["balances"])
        final_balances_2 = product_test_utils.get_final_balances(res[29]["result"]["balances"]["main_account"]["balances"])

        self.assertEqual("4.9997", final_balances["ACCRUED_INCOMING_INTEREST"])
        self.assertEqual("17.5339", final_balances_2["ACCRUED_INCOMING_INTEREST"])

    def test_daily_bonus_interest_accrue_interest_apply(self):
        start = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        end = datetime(year=2019, month=1, day=10, hour=9, tzinfo=timezone.utc)
        test_instance_params = {
            'base_interest_rate': '.002',
            'bonus_interest_rate': '.005',
            'bonus_interest_amount_threshold': '2500',
            'minimum_balance_maintenance_fee_waive': '5000',
            'flat_fee': '0',
        }
        instructions = [] 

        deposit_instruction = product_test_utils.create_deposit_instruction(
            amount="2475", timestamp=start.isoformat()
        )

        instructions.append(vault_caller.SimulationInstruction(start, deposit_instruction))

        res = self.make_simulate_contracts_call(
            start,
            end,
            default_template_params,
            test_instance_params,
            instructions,
        )

        final_balances = product_test_utils.get_final_balances(res[27]["result"]["balances"]["main_account"]["balances"])
        final_balances_2 = product_test_utils.get_final_balances(res[31]["result"]["balances"]["main_account"]["balances"])

        self.assertEqual("2504.8489", final_balances["DEFAULT"])
        self.assertEqual("2522.3828", final_balances_2["DEFAULT"]) 

    def test_bonus_interest_minimum_amount(self):
        start = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        end = datetime(year=2019, month=1, day=2, hour=9, tzinfo=timezone.utc)
        test_instance_params = {
            'base_interest_rate': '.002',
            'bonus_interest_rate': '.005',
            'bonus_interest_amount_threshold': '2500',
            'minimum_balance_maintenance_fee_waive': '5000',
            'flat_fee': '0',
        }
        instructions = [] 

        deposit_instruction = product_test_utils.create_deposit_instruction(
            amount="2475", timestamp=start.isoformat()
        )

        instructions.append(vault_caller.SimulationInstruction(start, deposit_instruction))

        res = self.make_simulate_contracts_call(
            start,
            end,
            default_template_params,
            test_instance_params,
            instructions,
        )

        final_balances = product_test_utils.get_final_balances(res[5]["result"]["balances"]["main_account"]["balances"])

        self.assertEqual("4.95", final_balances["ACCRUED_INCOMING_INTEREST"]) 
    
    def test_maintenance_fee_waived(self):
        start = datetime(year=2019, month=1, day=1, hour=9, tzinfo=timezone.utc)
        end = datetime(year=2019, month=2, day=2, hour=10, tzinfo=timezone.utc)
        test_instance_params = {
            'base_interest_rate': '.002',
            'bonus_interest_rate': '.005',
            'bonus_interest_amount_threshold': '2500',
            'minimum_balance_maintenance_fee_waive': '200',
            'flat_fee': '0',
        }
        instructions = []

        deposit_instruction = product_test_utils.create_deposit_instruction(
            amount="4950", timestamp=start.isoformat(), client_transaction_id="1"
        )
        instructions.append(vault_caller.SimulationInstruction(start, deposit_instruction))

        res = self.make_simulate_contracts_call(
            start,
            end,
            default_template_params,
            test_instance_params,
            instructions,
        )
        self.assertFalse(res[129]["result"]["posting_instruction_batches"])