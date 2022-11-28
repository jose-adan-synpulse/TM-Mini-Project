# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
from datetime import datetime
from collections import defaultdict
from unittest import TestCase
from unittest.mock import Mock, DEFAULT, ANY
from decimal import Decimal
from enum import Enum, unique
from typing import Any, Dict, List, Optional, Tuple
import time
from contextlib import ExitStack
from common.test_utils.contracts.unit.common import (
    ContractTest,
    mock_posting_instruction,
    mock_posting_instruction_batch,
)
from common.test_utils.contracts.unit.supervisor import run
from common.test_utils.contracts.unit.supervisor.types_extension import (
    DEFAULT_ADDRESS,
    DEFAULT_ASSET,
    Phase,
    PostingInstructionType,
    Tside,
    PostingInstructionBatch,
    ClientTransaction,
    ClientTransactionEffects,
    PostingInstructionBatchDirective,
    HookDirectives,
    AddAccountNoteDirective,
    AmendScheduleDirective,
    RemoveSchedulesDirective,
    WorkflowStartDirective,
    UpdateAccountEventTypeDirective,
)

DEFAULT_DENOMINATION = "GBP"
DEFAULT_TYPE = None
DEFAULT_PHASE = Phase.COMMITTED


def balance_dimensions(
    address=DEFAULT_ADDRESS,
    asset=DEFAULT_ASSET,
    denomination=DEFAULT_DENOMINATION,
    phase=DEFAULT_PHASE,
):
    return address, asset, denomination, phase


def mock_client_transaction(
    postings=None,
    authorised=Decimal(0),
    released=Decimal(0),
    settled=Decimal(0),
    unsettled=Decimal(0),
    cancelled=False,
):
    effect = ClientTransactionEffects()
    effect.authorised = authorised
    effect.released = released
    effect.settled = settled
    effect.unsettled = unsettled
    client_transaction = ClientTransaction(postings or [])
    client_transaction.effects.side_effect = mock_client_transaction_effects
    client_transaction.effects.return_value = defaultdict(lambda: effect)
    client_transaction.cancelled = cancelled
    return client_transaction


def mock_client_transaction_effects(timestamp=None):
    effect = ClientTransactionEffects()
    if timestamp:
        effect.authorised = Decimal(0)
        effect.released = Decimal(0)
        effect.settled = Decimal(0)
        effect.unsettled = Decimal(0)
        return defaultdict(lambda: effect)
    else:
        return DEFAULT


def create_posting_instruction_batch_directive(
    tside: str,
    amount: Decimal = Decimal(0),
    denomination: str = DEFAULT_DENOMINATION,
    from_account_address: str = "",
    from_account_id: str = "",
    to_account_address: str = "",
    to_account_id: str = "",
    client_transaction_id: str = "",
    override_all_restrictions: bool = False,
    value_timestamp: Optional[datetime] = None,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    batch_id: Optional[str] = None,
    client_batch_id: Optional[str] = None,
):

    """
    Create posting instruction batch directive with mocked posting instruction and posting
    instruction batch objects accessed by supervisor smart contract for supervisee
    Vault objects.
    return: PostingInstructionBatchDirectives
    """
    return PostingInstructionBatchDirective(
        posting_instruction_batch=mock_posting_instruction_batch(
            tside=tside,
            value_timestamp=value_timestamp,
            posting_instructions=[
                mock_posting_instruction(
                    tside=tside,
                    amount=amount,
                    denomination=denomination,
                    account_id=from_account_id,
                    address=from_account_address,
                    instruction_type=PostingInstructionType.CUSTOM_INSTRUCTION,
                    client_transaction_id=client_transaction_id,
                    override_all_restrictions=override_all_restrictions,
                    credit=False,
                    instruction_details=instruction_details,
                ),
                mock_posting_instruction(
                    tside=tside,
                    amount=amount,
                    denomination=denomination,
                    account_id=to_account_id,
                    address=to_account_address,
                    instruction_type=PostingInstructionType.CUSTOM_INSTRUCTION,
                    client_transaction_id=client_transaction_id,
                    override_all_restrictions=override_all_restrictions,
                    credit=True,
                    instruction_details=instruction_details,
                ),
            ],
            batch_details=batch_details,
            client_batch_id=client_batch_id,
            batch_id=batch_id,
        )
    )


def create_hook_directive(
    add_account_note_directives: Optional[List[AddAccountNoteDirective]] = None,
    amend_schedule_directives: Optional[List[AmendScheduleDirective]] = None,
    remove_schedules_directives: Optional[List[RemoveSchedulesDirective]] = None,
    workflow_start_directives: Optional[List[WorkflowStartDirective]] = None,
    posting_instruction_batch_directives: Optional[
        List[PostingInstructionBatchDirective]
    ] = None,
    update_account_event_type_directives: Optional[
        List[UpdateAccountEventTypeDirective]
    ] = None,
):
    """
    Create hook directive with a list of mock directives to be accessed by
    supervisor smart contract.
    return: HookDirectives
    """
    return HookDirectives(
        add_account_note_directives=add_account_note_directives,
        amend_schedule_directives=amend_schedule_directives,
        remove_schedules_directives=remove_schedules_directives,
        workflow_start_directives=workflow_start_directives,
        posting_instruction_batch_directives=posting_instruction_batch_directives,
        update_account_event_type_directives=update_account_event_type_directives,
    )


class OptionalValue:
    def __init__(self, value, is_set=True):
        self.value = value
        self.is_set = Mock(return_value=is_set)


@unique
class Direction(Enum):
    CREDIT = True
    DEBIT = False


class SupervisorContractTest(TestCase):
    @classmethod
    def setUpClass(self):
        self.smart_contracts = {}
        if hasattr(self, "contract_file"):
            with open(self.contract_file, "r", encoding="utf-8") as content_file:
                self.smart_contract = content_file.read()
        elif hasattr(self, "contract_files"):
            with ExitStack() as stack:
                for alias, fname in self.contract_files.items():
                    content_file = stack.enter_context(
                        open(fname, "r", encoding="utf-8")
                    )
                    smart_contract = content_file.read()
                    self.smart_contracts[alias] = smart_contract
                    if alias.lower() == "supervisor":
                        self.smart_contract = smart_contract

        def assert_no_call(self, *args, **kwargs):
            try:
                self.assert_called_with(*args, **kwargs)
            except AssertionError:
                return
            raise AssertionError(
                f"Expected {self._format_mock_call_signature(args, kwargs)} not to have been called"
            )

        Mock.assert_no_call = assert_no_call

    def setUp(self):
        self._started_at = time.time()

    def tearDown(self):
        self._elapsed_time = time.time() - self._started_at
        print(
            "{} ({}s)".format(
                self.id().rpartition(".")[2], round(self._elapsed_time, 2)
            )
        )

    def create_supervisee_mock(
        self,
        balance_ts: Optional[List[str]] = None,
        postings: Optional[PostingInstructionBatch] = None,
        creation_date: datetime = datetime(2019, 1, 1),
        client_transaction: Optional[Dict[Tuple, ClientTransaction]] = None,
        flags: List[str] = None,
        alias: str = None,
        hook_directives: Optional[List[Any]] = None,
        tside: Tside = None,
        **kwargs,
    ) -> Mock:
        """
        Create mock Vault object for supervisee using base unit test create_mock.
        """
        supervisee_mock = ContractTest().create_mock(
            balance_ts=balance_ts,
            postings=postings,
            creation_date=creation_date,
            client_transaction=client_transaction,
            flags=flags,
            tside=tside,
            alias=alias,
            **kwargs,
        )
        supervisee_mock.get_hook_directives.return_value = hook_directives
        return supervisee_mock

    def create_supervisor_mock(
        self, creation_date=datetime(2019, 1, 1), supervisees=None, **kwargs
    ):
        """
        Create mock Vault object for supervisor using base unit test create_mock.
        """

        supervisees = supervisees or {}

        def mock_get_last_execution_time(event_type):
            mock = Mock()
            if event_type in kwargs:
                mock.return_value = kwargs[event_type]
            else:
                mock.return_value = ANY
            return mock.return_value

        mock_supervisor_vault = Mock()
        mock_supervisor_vault.get_last_execution_time = mock_get_last_execution_time
        mock_supervisor_vault.get_plan_creation_date.return_value = creation_date
        mock_supervisor_vault.get_hook_execution_id.return_value = "MOCK_HOOK"
        mock_supervisor_vault.plan_id = "MOCK_PLAN"
        mock_supervisor_vault.supervisees = supervisees

        return mock_supervisor_vault

    def run_function(self, function_name: str, vault_object, *args, **kwargs):
        return run(self.smart_contract, function_name, vault_object, *args, **kwargs)

    @staticmethod
    def assert_no_side_effects(mock_vault):
        """
        Asserts that no postings, workflows or schedules were created/amended/deleted
        param mock_vault: Mock, vault mock after test has run
        return:
        """
        mock_vault.make_internal_transfer_instructions.assert_not_called()
        mock_vault.instruct_posting_batch.assert_not_called()
        mock_vault.start_workflow.assert_not_called()
        mock_vault.amend_schedule.assert_not_called()
