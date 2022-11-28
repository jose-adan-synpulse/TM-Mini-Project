from datetime import datetime
from common.test_utils.contracts.unit.common import ContractTest

CONTRACT_FILE = (
    "common/test_utils/contracts/unit/coverage_test/coverage_test_contract.py"
)


class CoverageTest(ContractTest):
    contract_file = CONTRACT_FILE

    def create_mock(self):
        return super().create_mock()

    def test_pre_posting_correct_return(self):
        # This test should only run one branch in the contract, resulting in a miss of 4 lines
        effective_time = datetime(2019, 1, 1)

        mock_vault = self.create_mock()

        self.run_function("pre_posting_code", mock_vault, [], effective_time)
