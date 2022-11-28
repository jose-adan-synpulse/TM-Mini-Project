from datetime import datetime
from common.test_utils.contracts.unit.common import ContractTest
from decimal import Decimal

FILE = "common/test_utils/common/contract_modules_examples/full_contract_with_shared_function.py"
MODULE_FILE = "common/test_utils/common/contract_modules_examples/contract_module.py"


class ContractSharedFunctionTest(ContractTest):
    contract_file = FILE
    linked_contract_modules = {
        "interest": {
            "path": MODULE_FILE,
        },
    }

    def test_get_parameter(self):
        mock_vault = self.create_mock(
            parameter_ts={
                "test_parameter_1": [(datetime.utcnow(), "test_val")],
                "test_parameter_2": [(datetime.utcnow(), 3)],
            }
        )
        parameter_1_value = self.run_function(
            "_get_parameter", mock_vault, vault=mock_vault, name="test_parameter_1"
        )

        self.assertEqual(parameter_1_value, "test_val")

        parameter_2_value = self.run_function(
            "_get_parameter", mock_vault, vault=mock_vault, name="test_parameter_2"
        )

        self.assertEqual(parameter_2_value, 3)

    def test_round_accrual(self):
        mock_vault = self.create_mock()
        parameter_value = self.run_function(
            "_round_accrual",
            mock_vault,
            vault=mock_vault,
            amount=Decimal(0.5343456),
        )
        self.assertEqual(parameter_value, Decimal("0.53435"))
