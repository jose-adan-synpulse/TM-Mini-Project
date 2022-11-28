# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
from datetime import datetime
from decimal import Decimal
from json import dumps

from common.test_utils.contracts.unit.common import ContractModuleTest
from common.test_utils.contracts.unit.types_extension import OptionalValue

CONTRACT_MODULE_FILE = (
    "common/test_utils/common/contract_modules_examples/contract_module.py"
)
DEFAULT_DATE = datetime(2021, 1, 1)


class ContractModuleTest(ContractModuleTest):
    contract_module_file = CONTRACT_MODULE_FILE

    def create_mock(
        self,
        balance_ts=None,
        postings=None,
        creation_date=DEFAULT_DATE,
        client_transaction=None,
        flags=None,
        **kwargs,
    ):
        balance_ts = balance_ts or []
        postings = postings or []
        client_transaction = client_transaction or {}
        flags = flags or []

        params = {
            key: {"value": value}
            for key, value in locals().items()
            if key not in self.locals_to_ignore
        }
        parameter_ts = self.param_map_to_timeseries(params, creation_date)
        return super().create_mock(
            balance_ts=balance_ts,
            parameter_ts=parameter_ts,
            postings=postings,
            creation_date=creation_date,
            client_transaction=client_transaction,
            flags=flags,
            **kwargs,
        )

    def test_round_accrual(self):
        result = self.run_function("round_accrual", None, amount=Decimal("5.555555"))
        self.assertEqual(result, Decimal("5.55556"))

    def test_get_parameter_latest(self):
        mock_vault = self.create_mock(test_parameter="test_value")
        result = self.run_function(
            function_name="get_parameter",
            vault_object=mock_vault,
            vault=mock_vault,
            name="test_parameter",
        )
        self.assertEqual(result, "test_value")

    def test_get_parameter_at_timestamp(self):
        mock_vault = self.create_mock(test_parameter="test_value")
        result = self.run_function(
            function_name="get_parameter",
            vault_object=mock_vault,
            vault=mock_vault,
            name="test_parameter",
            at=DEFAULT_DATE,
        )
        self.assertEqual(result, "test_value")

    def test_get_parameter_is_json(self):
        mock_vault = self.create_mock(test_parameter=dumps({"test_key": "test_value"}))
        result = self.run_function(
            function_name="get_parameter",
            vault_object=mock_vault,
            vault=mock_vault,
            name="test_parameter",
            is_json=True,
        )
        self.assertEqual(result, {"test_key": "test_value"})

    def test_get_parameter_optional(self):
        mock_vault = self.create_mock(
            test_parameter=OptionalValue(value="test_value", is_set=False)
        )
        result = self.run_function(
            function_name="get_parameter",
            vault_object=mock_vault,
            vault=mock_vault,
            name="test_parameter",
            optional=True,
            default_value="default_value",
        )
        self.assertEqual(result, "default_value")
