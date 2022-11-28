# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# standard libs
import time
from collections import defaultdict, namedtuple
from datetime import datetime
from decimal import Decimal
from os import getenv
from typing import DefaultDict, Dict, List, Optional, Tuple, Union
from unittest import TestCase
from unittest.mock import Mock, DEFAULT, ANY
from pathlib import Path

# third party
import coverage

# common
from common.test_utils.contracts.unit import run, ContractModuleRunner
from common.test_utils.contracts.unit.types_extension import (
    DEFAULT_ADDRESS,
    DEFAULT_ASSET,
    Balance,
    BalanceDefaultDict,
    CalendarEvent,
    CalendarEvents,
    ClientTransaction,
    ClientTransactionEffects,
    Parameter,
    Phase,
    PostingInstruction,
    PostingInstructionBatch,
    PostingInstructionType,
    Tside,
    OptionalValue,
)

DEFAULT_DENOMINATION = "GBP"
DEFAULT_TYPE = None
DEFAULT_PHASE = Phase.COMMITTED
POSTING_CLIENT_ID = "client_id"
CLIENT_TRANSACTION_ID = "MOCK_POSTING"
ACCOUNT_ID = "default_account"
POSTING_ID = "posting_id"
DEFAULT_DENOM = "GBP"

# Coverage information for unit tests is available, but disabled by default because clients report
# that it prevents IDE debugging in unit test mode. Enable by setting this environment variable.
# Thought Machine set this in the build system configuration file (.plzconfig)
ENABLE_COVERAGE = getenv("INCEPTION_UNIT_TEST_COVERAGE", False)

# Base directory below which coverage information will be saved, when enabled
COVERAGE_TOP_DIR = "/tmp"

BalanceDimensions = namedtuple(
    "BalanceDimensions",
    ["address", "asset", "denomination", "phase"],
    defaults=[DEFAULT_ADDRESS, DEFAULT_ASSET, DEFAULT_DENOM, Phase.COMMITTED],
)


def balance(tside, net=None, debit=None, credit=None):
    """
    Given a net, or a debit/credit pair, return an equivalent Balance object
    Direction of net is derived from Tside of account (asset vs. liability)
    :param net:
    :param debit:
    :param credit:
    """
    if net is None:
        net = (
            Decimal(credit) - Decimal(debit)
            if (tside == Tside.LIABILITY)
            else Decimal(debit) - Decimal(credit)
        )
    else:
        net = Decimal(net)
        if tside == Tside.LIABILITY:
            credit = net
            debit = Decimal(0)
        else:
            credit = Decimal(0)
            debit = net
    return Balance(debit=debit, credit=credit, net=net)


def mock_posting_instruction_batch(
    tside,
    value_timestamp=datetime(2019, 1, 1),
    batch_id="MOCK_POSTING_BATCH",
    posting_instructions=None,
    batch_details=None,
    client_batch_id=None,
):
    posting_instructions = posting_instructions or []

    pib = PostingInstructionBatch(
        value_timestamp=value_timestamp,
        batch_id=batch_id,
        posting_instructions=posting_instructions or [],
        batch_details=batch_details or {},
        client_batch_id=client_batch_id,
    )

    # PIB balances are just merged posting instruction balances
    balances = BalanceDefaultDict(lambda: Balance())
    for posting_instruction in posting_instructions:
        balances += posting_instruction.balances()

    pib.balances = Mock(return_value=balances)

    return pib


def mock_posting_instruction(
    tside,
    address=DEFAULT_ADDRESS,
    amount: Optional[Decimal] = Decimal(0),
    credit=True,
    denomination=DEFAULT_DENOMINATION,
    instruction_details: Dict[str, str] = None,
    instruction_type: PostingInstructionType = PostingInstructionType.HARD_SETTLEMENT,
    phase=Phase.COMMITTED,
    client_id=POSTING_CLIENT_ID,
    client_transaction_id=CLIENT_TRANSACTION_ID,
    account_id=ACCOUNT_ID,
    value_timestamp=None,
    asset=DEFAULT_ASSET,
    final=False,
    posting_id=POSTING_ID,
    mocks_as_any: bool = False,
    override_all_restrictions=False,
    unsettled_amount: Decimal = Decimal(0),
    advice=False,
    original_credit: Optional[bool] = None,
):

    amount = Decimal(amount) if amount else None
    unsettled_amount = Decimal(unsettled_amount)
    instruction_details = instruction_details or {}
    instruction = PostingInstruction(
        account_address=address,
        account_id=account_id,
        asset=asset,
        amount=amount,
        client_transaction_id=client_transaction_id,
        denomination=denomination,
        credit=credit,
        phase=phase,
        override_all_restrictions=override_all_restrictions,
        instruction_details=instruction_details,
        type=instruction_type,
        id=posting_id,
        advice=advice,
    )
    instruction.client_id = client_id
    instruction.final = final
    instruction.value_timestamp = value_timestamp

    if instruction_type == PostingInstructionType.CUSTOM_INSTRUCTION:
        instruction.custom_instruction_grouping_key = client_transaction_id

    # This is used to return ANY instead of mocks so that comparisons between what is generated
    # in the tests and the expected returns match
    if mocks_as_any:
        instruction.batch_details = ANY
        instruction.client_batch_id = ANY
        instruction.value_timestamp = ANY
        instruction.batch_id = ANY
        instruction.balances = ANY

        return instruction

    balances = {}
    if instruction.type == PostingInstructionType.CUSTOM_INSTRUCTION:
        dimensions = BalanceDimensions(address, asset, denomination, phase)
        value = balance(
            debit=0 if credit else amount, credit=amount if credit else 0, tside=tside
        )
        balances = {dimensions: value}

    elif instruction.type == PostingInstructionType.AUTHORISATION:
        dimensions = BalanceDimensions(
            DEFAULT_ADDRESS,
            asset,
            denomination,
            phase.PENDING_IN if credit else phase.PENDING_OUT,
        )
        amount2 = amount if amount else 0
        value = balance(
            debit=0 if credit else amount2, credit=amount2 if credit else 0, tside=tside
        )
        balances = {dimensions: value}

    elif instruction.type in [
        PostingInstructionType.HARD_SETTLEMENT,
        PostingInstructionType.TRANSFER,
    ]:
        dimensions = BalanceDimensions(
            DEFAULT_ADDRESS, asset, denomination, phase.COMMITTED
        )
        amount2 = amount if amount else 0
        value = balance(
            debit=0 if credit else amount2, credit=amount2 if credit else 0, tside=tside
        )
        balances = {dimensions: value}

    # TODO: handle absolute auth adjust amounts
    elif instruction.type == PostingInstructionType.AUTHORISATION_ADJUSTMENT:
        # original_credit should be true for inbound auth and false for outbound auth
        if original_credit:
            if credit:  # increasing an inbound auth
                value = balance(debit=Decimal(0), credit=amount, tside=tside)
            else:  # decreasing an inbound auth
                value = balance(debit=abs(amount), credit=Decimal(0), tside=tside)
        else:
            if credit:  # decreasing an outbound auth
                value = balance(debit=Decimal(0), credit=abs(amount), tside=tside)
            else:  # increasing an outbound auth
                value = balance(debit=amount, credit=Decimal(0), tside=tside)

        dimensions = BalanceDimensions(
            DEFAULT_ADDRESS,
            asset,
            denomination,
            phase.PENDING_IN if original_credit else phase.PENDING_OUT,
        )
        balances = {dimensions: value}

    elif instruction.type == PostingInstructionType.SETTLEMENT:
        # credit should be true for inbound auth and false for outbound auth
        settlement_amount = amount or unsettled_amount
        if final:
            auth_amount = unsettled_amount
        else:
            auth_amount = min(settlement_amount, unsettled_amount)

        # this zeroes the pending balance
        value_pending = balance(
            debit=auth_amount if credit else 0,
            credit=0 if credit else auth_amount,
            tside=tside,
        )
        # this debits/credits the
        value_committed = balance(
            debit=0 if credit else settlement_amount,
            credit=settlement_amount if credit else 0,
            tside=tside,
        )

        dimensions_pending = BalanceDimensions(
            DEFAULT_ADDRESS,
            asset,
            denomination,
            phase.PENDING_IN if credit else phase.PENDING_OUT,
        )

        dimensions_committed = BalanceDimensions(
            DEFAULT_ADDRESS, asset, denomination, phase.COMMITTED
        )

        balances = {
            dimensions_pending: value_pending,
            dimensions_committed: value_committed,
        }

    elif instruction.type == PostingInstructionType.RELEASE:
        # credit should be true for inbound auth and false for outbound auth
        # this zeroes the pending balance
        value_pending = balance(
            debit=unsettled_amount if credit else 0,
            credit=0 if credit else unsettled_amount,
            tside=tside,
        )

        dimensions_pending = BalanceDimensions(
            DEFAULT_ADDRESS,
            asset,
            denomination,
            phase.PENDING_IN if credit else phase.PENDING_OUT,
        )

        balances = {
            dimensions_pending: value_pending,
        }

    instruction.balances = Mock(
        return_value=defaultdict(
            lambda: Balance(Decimal(0), Decimal(0), Decimal(0)), balances
        )
    )

    return instruction


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
    start_time=None,
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
    if start_time is not None:
        client_transaction.start_time = start_time
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


class ContractTest(TestCase):

    contract_file = ""
    linked_contract_modules = {}
    contract_module_runners = {}
    side = None
    locals_to_ignore = [
        "balance_ts",
        "parameter_ts",
        "postings",
        "client_transaction",
        "flags",
        "self",
        "cls",
        "kwargs",
        "__class__",
    ]

    @classmethod
    def setUpClass(cls):
        contract_file = cls.contract_file
        with open(contract_file, "r", encoding="utf-8") as content_file:
            cls.smart_contract = content_file.read()
        cls.setUpContractModules()

        if ENABLE_COVERAGE:
            cls.cov = coverage.Coverage(
                # Dont show coverage for files other than the SC
                include=[cls.contract_file]
            )
            cls.cov.start()

    @classmethod
    def setUpContractModules(cls):
        """
        cls.linked_contract_modules should be of the format:
            {
                "alias": {
                    "path":"path_to_module.py",
                },
                ...
            }
        """
        for alias, module_dict in cls.linked_contract_modules.items():
            with open(module_dict["path"], "r", encoding="utf-8") as module_file:
                module_code = module_file.read()
                module_runner = ContractModuleRunner(module_code)
                cls.contract_module_runners[alias] = module_runner

    def setUp(self):
        self._started_at = time.time()

    def tearDown(self):
        self._elapsed_time = time.time() - self._started_at
        print(
            "{} ({}s)".format(
                self.id().rpartition(".")[2], round(self._elapsed_time, 2)
            )
        )

    @classmethod
    def tearDownClass(cls):
        if ENABLE_COVERAGE:
            cls.cov.stop()

            dir_name = str(Path(cls.contract_file).stem) + "_coverage_reports"
            report_path = Path(COVERAGE_TOP_DIR) / dir_name / "report"

            report_path.parent.mkdir(parents=True, exist_ok=True)
            with report_path.open(mode="w", encoding="utf-8") as report:
                cls.cov.report(file=report)
            cls.cov.xml_report(outfile=str(report_path) + ".xml")

    def create_mock(
        self,
        balance_ts: Optional[List[str]] = None,
        parameter_ts: Optional[Dict[str, List[Tuple[datetime, Parameter]]]] = None,
        postings: Optional[PostingInstructionBatch] = None,
        creation_date: datetime = datetime(2019, 1, 1),
        client_transaction: Optional[Dict[Tuple, ClientTransaction]] = None,
        flags: Optional[
            Union[List[str], Dict[str, List[Tuple[datetime, bool]]]]
        ] = None,
        account_id: str = "Main account",
        calendar_events: Optional[List[CalendarEvent]] = None,
        **kwargs,
    ) -> Mock:
        """
        Create mock Vault object for the test

        All parameters are optional apart from account_id and creation_date.

        :param balance_ts: Balance time series
        :param parameter_ts: dict where key is param name and entry is list of (dt, value) tuples
        :param postings: Posting instruction batch
        :param creation_date: Account creation date
        :param client_transaction: Client transaction
        :param flags: Either a list of flag names which are active for all time, or a dict where the
        keys are the flag name(s) and the value is a list of (datetime, bool) tuples. The bool is
        True for active and False for inactive.
        :param account_id: Account ID
        :param calendar_events: A list of calendar events
        :param kwargs: Remaining arguments are interpreted flexibly e.g. parameter name+value etc.
        """

        parameter_ts = parameter_ts or {}
        balance_ts = balance_ts or []
        flags = flags or []
        postings = postings or []
        client_transaction = client_transaction or {}
        calendar_events = calendar_events or []

        def mock_get_balance_timeseries() -> TimeSeries:
            return TimeSeries(
                balance_ts, return_on_empty=BalanceDefaultDict(lambda: Balance())
            )

        def mock_get_parameter_timeseries(
            name: str,
        ) -> TimeSeries:
            if name in parameter_ts:
                parameter_timeseries = parameter_ts[name]
            elif name in kwargs:
                parameter_timeseries = [(creation_date, kwargs[name])]
            else:
                parameter_timeseries = [(creation_date, [])]
            return TimeSeries(parameter_timeseries)

        def mock_get_calendar_events(calendar_ids: List[str]) -> CalendarEvents:
            events = [
                event for event in calendar_events if event.calendar_id in calendar_ids
            ]
            return events

        def mock_get_flag_timeseries(flag: str) -> TimeSeries:
            if flag in flags:
                if isinstance(flags, list):
                    # flags is a simple list of flag names, interpret as flag active from
                    # creation date
                    flag_timeseries = [(creation_date, True)]
                else:
                    # flag settings have been supplied as a timeseries
                    flag_timeseries = flags[flag]
            else:
                # No setting supplied for flag, so it is False
                flag_timeseries = [(creation_date, False)]
            return TimeSeries(flag_timeseries, return_on_empty=False)

        def mock_get_last_execution_time(event_type: str) -> datetime:
            return kwargs.get(event_type)

        def mock_internal_transfer_instruction_side_effect(*args, **kwargs):
            return [kwargs["client_transaction_id"]]

        mock_vault = Mock()
        mock_vault.make_internal_transfer_instructions.side_effect = (
            mock_internal_transfer_instruction_side_effect
        )
        mock_vault.get_postings.return_value = postings
        mock_vault.get_balance_timeseries.side_effect = mock_get_balance_timeseries
        mock_vault.get_parameter_timeseries.side_effect = mock_get_parameter_timeseries
        mock_vault.get_flag_timeseries.side_effect = mock_get_flag_timeseries
        mock_vault.get_client_transactions.return_value = client_transaction
        mock_vault.get_account_creation_date.return_value = creation_date
        mock_vault.get_last_execution_time = mock_get_last_execution_time
        mock_vault.get_hook_execution_id.return_value = "MOCK_HOOK"
        mock_vault.instruct_posting_batch.return_value = ANY
        mock_vault.account_id = account_id
        mock_vault.get_calendar_events.side_effect = mock_get_calendar_events
        mock_vault.get_alias.return_value = kwargs.get("alias")
        mock_vault.modules = self.contract_module_runners

        return mock_vault

    def run_function(self, function_name: str, vault_object, *args, **kwargs):
        return run(
            compile(self.smart_contract, self.contract_file, "exec"),
            function_name,
            vault_object,
            *args,
            **kwargs,
        )

    def balance(self, net=None, debit=None, credit=None):
        """
        Given a net, or a debit/credit pair, return an equivalent Balance object
        Direction of net is derived from Tside of account (asset vs. liability)
        :param net:
        :param debit:
        :param credit:
        """
        return balance(net=net, debit=debit, credit=credit, tside=self.side)

    def mock_posting_instruction_batch(
        self,
        value_timestamp=datetime(2019, 1, 1),
        batch_id="MOCK_POSTING_BATCH",
        posting_instructions=None,
        denomination=DEFAULT_DENOMINATION,
        batch_details=None,
        client_batch_id=None,
    ):

        return mock_posting_instruction_batch(
            self.side,
            value_timestamp=value_timestamp,
            batch_id=batch_id,
            posting_instructions=posting_instructions,
            batch_details=batch_details,
            client_batch_id=client_batch_id,
        )

    def mock_posting_instruction(
        self,
        address=DEFAULT_ADDRESS,
        amount: Optional[Decimal] = Decimal(0),
        credit=True,
        denomination=DEFAULT_DENOMINATION,
        instruction_details: Dict[str, str] = None,
        instruction_type: PostingInstructionType = PostingInstructionType.HARD_SETTLEMENT,
        phase=Phase.COMMITTED,
        client_id=POSTING_CLIENT_ID,
        client_transaction_id=CLIENT_TRANSACTION_ID,
        account_id=ACCOUNT_ID,
        value_timestamp=None,
        asset=DEFAULT_ASSET,
        final=False,
        posting_id=POSTING_ID,
        mocks_as_any: bool = False,
        override_all_restrictions=False,
        unsettled_amount: Decimal = Decimal(0),
        advice=False,
        original_credit: Optional[bool] = None,
    ):
        """
        Creates a mock posting instruction to be fed into unit tests. All parameters as per
        PostingInstruction type, except for those documented below
        :param address:
        :param account_id:
        :param asset:
        :param amount:
        :param client_id:
        :param client_transaction_id:
        :param credit:
        :param denomination:
        :param final:
        :param posting_id:
        :param instruction_details:
        :param mocks_as_any: if set to True, all PostingInstruction methods return 'ANY'. Use
        if you need to compare mocks and actual contract outputs
        :param override_all_restrictions:
        :param phase:
        :param instruction_type:
        :param value_timestamp:
        :param unsettled_amount: use to mock instruction balances for secondary instructions where
         amount may be None. In Vault this would be calculated using the client transaction, but
         for unit tests we just want to specify the amount
        :param advice:
        :param original_credit: set to True for auth_adjust to inbound auth, False for auth_adjust
         to outbound_auth
        :return:
        """
        denomination = denomination or self.default_denom
        return mock_posting_instruction(
            self.side,
            address=address,
            amount=amount,
            credit=credit,
            denomination=denomination,
            instruction_details=instruction_details,
            instruction_type=instruction_type,
            phase=phase,
            client_id=client_id,
            client_transaction_id=client_transaction_id,
            account_id=account_id,
            value_timestamp=value_timestamp,
            asset=asset,
            final=final,
            posting_id=posting_id,
            mocks_as_any=mocks_as_any,
            override_all_restrictions=override_all_restrictions,
            unsettled_amount=unsettled_amount,
            advice=advice,
            original_credit=original_credit,
        )

    def auth(
        self,
        amount=Decimal(0),
        credit=False,
        denomination="",
        posting_id=POSTING_ID,
        instruction_details=None,
    ):
        instruction_details = instruction_details or {}

        return self.mock_posting_instruction(
            amount=amount,
            credit=credit,
            denomination=denomination or self.default_denom,
            posting_id=posting_id,
            instruction_details=instruction_details,
            instruction_type=PostingInstructionType.AUTHORISATION,
        )

    def inbound_auth(
        self,
        amount=Decimal(0),
        denomination="",
        posting_id=POSTING_ID,
        instruction_details=None,
    ):
        return self.auth(
            amount=amount,
            credit=True,
            denomination=denomination or self.default_denom,
            posting_id=posting_id,
            instruction_details=instruction_details,
        )

    def outbound_auth(
        self,
        amount=Decimal(0),
        denomination="",
        posting_id=POSTING_ID,
        instruction_details=None,
    ):
        return self.auth(
            amount=amount,
            credit=False,
            denomination=denomination or self.default_denom,
            posting_id=posting_id,
            instruction_details=instruction_details,
        )

    def inbound_auth_adjust(
        self,
        amount: Decimal = Decimal(0),
        denomination: str = "",
        client_transaction_id: str = CLIENT_TRANSACTION_ID,
    ) -> PostingInstruction:

        credit = amount > 0

        return self.mock_posting_instruction(
            amount=abs(amount),
            credit=credit,
            denomination=denomination or self.default_denom,
            client_transaction_id=client_transaction_id,
            instruction_type=PostingInstructionType.AUTHORISATION_ADJUSTMENT,
            original_credit=True,  # inbound auth is always credit True
        )

    def outbound_auth_adjust(self, amount=Decimal(0), denomination=""):

        credit = not (amount > 0)

        return self.mock_posting_instruction(
            amount=abs(amount),
            credit=credit,
            denomination=denomination or self.default_denom,
            instruction_type=PostingInstructionType.AUTHORISATION_ADJUSTMENT,
            original_credit=False,  # outbound auth is always credit False
        )

    def settle(
        self,
        amount,
        final,
        denomination="",
        posting_id=POSTING_ID,
        credit=False,
        instruction_details=None,
        unsettled_amount=Decimal(0),
    ):
        instruction_details = instruction_details or {}

        return self.mock_posting_instruction(
            amount=amount,
            credit=credit,
            denomination=denomination or self.default_denom,
            posting_id=posting_id,
            instruction_details=instruction_details,
            final=final,
            instruction_type=PostingInstructionType.SETTLEMENT,
            unsettled_amount=unsettled_amount,
        )

    def settle_inbound_auth(
        self,
        amount,
        final,
        denomination="",
        posting_id=POSTING_ID,
        instruction_details=None,
        unsettled_amount=Decimal(0),
    ):
        instruction_details = instruction_details or {}

        # settlements retain original auth credit
        return self.settle(
            amount=amount,
            credit=True,
            denomination=denomination or self.default_denom,
            posting_id=posting_id,
            instruction_details=instruction_details,
            final=final,
            unsettled_amount=unsettled_amount,
        )

    def settle_outbound_auth(
        self,
        amount,
        final,
        denomination="",
        posting_id=POSTING_ID,
        instruction_details=None,
        unsettled_amount=Decimal(0),
    ):
        instruction_details = instruction_details or {}

        # settlements retain original auth credit
        return self.settle(
            amount=amount,
            credit=False,
            denomination=denomination or self.default_denom,
            posting_id=posting_id,
            instruction_details=instruction_details,
            final=final,
            unsettled_amount=unsettled_amount,
        )

    def release(
        self,
        denomination="",
        unsettled_amount=Decimal(0),
        credit=False,
        instruction_details=None,
    ):
        instruction_details = instruction_details or {}

        return self.mock_posting_instruction(
            credit=credit,
            denomination=denomination or self.default_denom,
            unsettled_amount=unsettled_amount,
            instruction_type=PostingInstructionType.RELEASE,
            instruction_details=instruction_details,
        )

    def release_inbound_auth(
        self,
        denomination="",
        unsettled_amount=Decimal(0),
        instruction_details=None,
    ):
        # releases retain original auth credit - which is True for inbound_auth
        # hence release_inbound_auth credit is True
        return self.release(
            denomination,
            unsettled_amount,
            credit=True,
            instruction_details=instruction_details,
        )

    def release_outbound_auth(
        self,
        denomination="",
        unsettled_amount=Decimal(0),
        instruction_details=None,
    ):
        # releases retain original auth credit - which is False for outbound_auth
        # hence release_outbound_auth credit is False
        return self.release(
            denomination,
            unsettled_amount,
            credit=False,
            instruction_details=instruction_details,
        )

    # TODO: add inbound/outbound hard settlement methods to avoid errors with credit
    def hard_settlement(self, amount, denomination="", credit=False, advice=False):
        return self.mock_posting_instruction(
            amount=amount,
            credit=credit,
            advice=advice,
            denomination=denomination or self.default_denom,
            instruction_type=PostingInstructionType.HARD_SETTLEMENT,
        )

    # TODO: add inbound/outbound transfer methods to avoid errors with credit
    def transfer(self, amount, denomination="", credit=False):
        return self.mock_posting_instruction(
            amount=amount,
            credit=credit,
            denomination=denomination or self.default_denom,
            instruction_type=PostingInstructionType.TRANSFER,
        )

    def custom_instruction(
        self,
        amount,
        account_address=DEFAULT_ADDRESS,
        asset=DEFAULT_ASSET,
        client_transaction_id=None,
        credit=False,
        denomination="",
        value_timestamp=datetime(1970, 1, 1),
        phase=Phase.COMMITTED,
    ):

        return self.mock_posting_instruction(
            amount=amount,
            address=account_address,
            asset=asset,
            client_transaction_id=client_transaction_id,
            credit=credit,
            denomination=denomination or self.default_denom,
            phase=phase,
            instruction_type=PostingInstructionType.CUSTOM_INSTRUCTION,
            value_timestamp=value_timestamp,
        )

    def init_balances(
        self,
        dt: Optional[datetime] = datetime(2019, 1, 1),
        balance_defs: Optional[List[Dict[str, str]]] = None,
    ) -> List[Tuple[datetime, DefaultDict[Tuple[str, str, str, Phase], Balance]]]:
        """
        Creates a simple balance timeseries with a single date
        :param dt: the date for which the balances are initialised
        :param balance_defs: List(dict) the balances to define for this date. Each def is a dict
         with 'address', 'denomination' 'phase' and 'asset' attributes for dimensions and 'net,
          'dr', 'cr'. Dimensions default to their default value as per Postings/Contracts API.
        Rest is as per the `balance` helper
        :return: List of datetime - balance dictionary tuples representing a balance timeseries
        """
        balance_defs = balance_defs or []
        balance_dict = BalanceDefaultDict(
            lambda: Balance(),
            {
                balance_dimensions(
                    address=balance_def.get("address", DEFAULT_ADDRESS).upper(),
                    denomination=balance_def.get("denomination", DEFAULT_DENOMINATION),
                    phase=balance_def.get("phase", Phase.COMMITTED),
                    asset=balance_def.get("asset", DEFAULT_ASSET),
                ): balance(
                    self.side,
                    net=balance_def.get("net"),
                    debit=balance_def.get("dr"),
                    credit=balance_def.get("cr"),
                )
                for balance_def in balance_defs
            },
        )
        return [(dt, balance_dict)]

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

    @staticmethod
    def param_map_to_timeseries(param_map, default_dt):
        def get_param_value(param_details, value):
            if param_details.get("optional", False):
                return OptionalValue(
                    value=value, is_set=value != param_details["default_value"]
                )
            return value

        param_timeseries = {}
        for param_name, param_details in param_map.items():
            if type(param_details["value"]) == list:
                param_timeseries[param_name] = [
                    (
                        timeseries_entry[0],
                        get_param_value(param_details, timeseries_entry[1]),
                    )
                    for timeseries_entry in param_details["value"]
                ]
            else:
                param_timeseries[param_name] = [
                    (default_dt, get_param_value(param_details, param_details["value"]))
                ]

        return param_timeseries


class ContractModuleTest(ContractTest):
    contract_module_file = ""

    @classmethod
    def setUpClass(cls):
        contract_module_file = cls.contract_module_file
        with open(contract_module_file, "r", encoding="utf-8") as content_file:
            cls.contract_module = content_file.read()

        if ENABLE_COVERAGE:
            cls.cov = coverage.Coverage(
                # Dont show coverage for files other than the contract module
                include=[cls.contract_module_file]
            )
            cls.cov.start()

    @classmethod
    def tearDownClass(cls):
        if ENABLE_COVERAGE:
            cls.cov.stop()

            dir_name = str(Path(cls.contract_module_file).stem) + "_coverage_reports"
            report_path = Path(COVERAGE_TOP_DIR) / dir_name / "report"

            report_path.parent.mkdir(parents=True, exist_ok=True)
            with report_path.open(mode="w", encoding="utf-8") as report:
                cls.cov.report(file=report)
            cls.cov.xml_report(outfile=str(report_path) + ".xml")

    def run_function(self, function_name: str, vault_object, *args, **kwargs):
        return run(
            compile(self.contract_module, self.contract_module_file, "exec"),
            function_name,
            vault_object,
            contract_module=True,
            *args,
            **kwargs,
        )


class TimeSeries(list):
    def __init__(self, items, return_on_empty=None):
        super().__init__(items)
        self.return_on_empty = return_on_empty

    def at(self, timestamp, inclusive=True):
        for entry in reversed(self):
            if entry[0] <= timestamp:
                if inclusive or entry[0] < timestamp:
                    return entry[1]

        if self.return_on_empty is not None:
            return self.return_on_empty

        raise ValueError(f"No value in timeseries at {timestamp}")

    def before(self, timestamp):
        return self.at(timestamp, inclusive=False)

    def latest(self):
        if not self:
            if self.return_on_empty is not None:
                return self.return_on_empty
        return self[-1][1]

    def all(self):  # noqa: A003
        return [item for item in self]
