# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# standard libs
import calendar
import collections
import datetime
import decimal
import json
import math
import typing
from collections import defaultdict
from enum import Enum
from typing import DefaultDict
from unittest.mock import Mock

# third party
from dateutil import parser, relativedelta

# Fixed Types
DEFAULT_ADDRESS = "DEFAULT"
DEFAULT_ASSET = "COMMERCIAL_BANK_MONEY"
TRANSACTION_REFERENCE_FIELD_NAME = "description"


class vault:
    def __init__(self):
        self.account_id = ""
        self.tside = Tside().ASSET

    def get_parameter_timeseries(*, name=None):
        return ParameterTimeseries()

    def get_balance_timeseries():
        return BalanceTimeseries()

    def get_flag_timeseries(*, name=None):
        return FlagTimeseries()

    def make_internal_transfer_instructions(
        amount,
        denomination,
        client_transaction_id,
        from_account_id,
        from_account_address,
        to_account_id,
        to_account_address,
        pics,
        instruction_details,
        asset,
        override_all_restrictions,
    ):
        return PostingInstruction()

    def get_last_execution_time(*, event_type):
        return datetime.datetime()

    def get_postings(*, include_proposed):
        return [PostingInstruction()]

    def get_client_transactions(*, include_proposed):
        return [ClientTransaction()]

    def get_posting_batches(*, include_proposed):
        return [PostingInstructionBatch()]

    def add_account_note(
        *, body, note_type, is_visible_to_customer, date, idempotency_key
    ):
        return

    def amend_schedule(*, event_type, new_schedule):
        return

    def get_account_creation_date():
        return datetime.datetime()

    def get_hook_execution_id():
        return ""

    def remove_schedule(*, event_type):
        return

    def start_workflow(*, workflow, context, idempotency_key):
        return

    def instruct_posting_batch(
        *,
        posting_instructions,
        batch_details,
        client_batch_id,
        effective_date,
        request_id
    ):
        return

    def localize_datetime(*, dt):
        return datetime.datetime()

    def get_permitted_denominations():
        return [""]


# Exceptions
class InvalidContractParameter(Exception):
    pass


class Rejected(Exception):
    def __init__(self, message, reason_code):
        self.message = message
        self.reason_code = reason_code


# Enums
class Level(Enum):
    GLOBAL = "1"
    INSTANCE = "3"
    TEMPLATE = "2"


class Features(Enum):
    CARD = "4"
    INVESTMENT = "7"
    JOINT_ACCOUNT = "6"
    MANDATES = "1"
    MULTIPLE_OWNERS = "3"
    SUB_ACCOUNTS = "5"


class NoteType(Enum):
    RAW_TEXT = "1"
    REASON_CODE = "2"


class NumberKind(Enum):
    MONEY = "money"
    MONTHS = "months"
    PERCENTAGE = "percentage"
    PLAIN = "plain"


class Phase(Enum):
    COMMITTED = "committed"
    PENDING_IN = "pending_in"
    PENDING_OUT = "pending_out"


class PostingInstructionType(Enum):
    AUTHORISATION = "Authorisation"
    AUTHORISATION_ADJUSTMENT = "AuthorisationAdjustment"
    CUSTOM_INSTRUCTION = "CustomInstruction"
    HARD_SETTLEMENT = "HardSettlement"
    RELEASE = "Release"
    SETTLEMENT = "Settlement"
    TRANSFER = "Transfer"


class RejectedReason(Enum):
    AGAINST_TNC = "3"
    CLIENT_CUSTOM_REASON = "4"
    INSUFFICIENT_FUNDS = "1"
    WRONG_DENOMINATION = "2"


class Tside(Enum):
    ASSET = "1"
    LIABILITY = "2"
    UNDEFINED = "0"


class UpdatePermission(Enum):
    FIXED = "1"
    OPS_EDITABLE = "2"
    USER_EDITABLE = "3"
    USER_EDITABLE_WITH_OPS_PERMISSION = "4"


class AccountIdShape:
    pass


class Balance:
    def __init__(self, credit=None, debit=None, net=None):
        self.credit = credit
        self.debit = debit
        self.net = net

    def _adjust(self, credit=None, debit=None, net=None):
        if credit:
            self.credit += credit
        if debit:
            self.debit += debit
        if net:
            self.net += net


class BalanceDefaultDict(defaultdict):
    pass


class BalanceTimeseries:
    def __init__(self, iterable=None):
        self.iterable = iterable

    def at():
        return Balance()

    def before():
        return Balance()

    def latest():
        return Balance()

    def all():
        return Balance()


class ClientTransaction(list):
    def __init__(self, postings):
        super().__init__(postings)
        self.is_custom = Mock()
        self.cancelled = Mock()
        self.start_time = datetime.datetime
        self.balances = Mock()

    def effects(timestamp):
        return ClientTransactionEffectsDefaultDict()

    def balances(timestamp):
        return BalanceDefaultDict()


class ClientTransactionEffects:
    def __init__(
        self,
    ):
        self.authorised = Mock()
        self.released = Mock()
        self.settled = Mock()
        self.unsettled = Mock()


class ClientTransactionEffectsDefaultDict(defaultdict):
    pass


class DateShape:
    def __init__(self, min_date=None, max_date=None):
        self.min_date = min_date if min_date else Mock()
        self.max_date = max_date if max_date else Mock()


class DenominationShape:
    def __init__(
        self,
    ):
        self.permitted_denominations = Mock()


class FlagTimeseries:
    def __init__(self, iterable=None):
        self.iterable = iterable

    def at():
        return True

    def before():
        return True

    def latest():
        return True

    def all():
        return True


class NumberShape:
    def __init__(self, kind=None, min_value=None, max_value=None, step=None):
        self.kind = kind
        self.min_value = min_value
        self.max_value = max_value
        self.step = step

    def __call__(self, kind=None, min_value=None, max_value=None, step=None):
        if kind:
            self.kind = kind
        if min_value:
            self.min_value = min_value
        if max_value:
            self.max_value = max_value
        if step:
            self.step = step


class OptionalShape:
    def __init__(self, shape=None):
        self.shape = shape


class OptionalValue:
    def __init__(self, value=None):
        self.value = value
        self.is_set = Mock()


class Parameter:
    def __init__(
        self,
        name=None,
        description=None,
        display_name=None,
        level=None,
        value=None,
        default_value=None,
        update_permission=None,
        derived=None,
        shape=None,
    ):
        self.name = name
        self.description = description
        self.display_name = display_name
        self.level = level
        self.value = value
        self.default_value = default_value
        self.update_permission = update_permission
        self.derived = derived
        self.shape = shape


class ParameterTimeseries:
    def __init__(self, iterable=None):
        self.iterable = iterable

    def at():
        return Parameter()

    def before():
        return Parameter()

    def latest():
        return Parameter()

    def all():
        return Parameter()


class PostingInstruction:
    def __init__(
        self,
        account_address=None,
        account_id=None,
        amount=None,
        asset=None,
        credit=None,
        denomination=None,
        final=None,
        phase=None,
        id=None,
        type=None,
        client_transaction_id=None,
        instruction_details=None,
        pics=None,
        custom_instruction_grouping_key=None,
        override_all_restrictions=None,
        client_id=None,
        advice=None,
    ):
        self.account_address = account_address
        self.account_id = account_id
        self.amount = amount
        self.asset = asset
        self.credit = credit
        self.denomination = denomination
        self.final = final
        self.phase = phase
        self.id = id
        self.type = type
        self.client_transaction_id = client_transaction_id
        self.instruction_details = instruction_details
        self.pics = pics
        self.custom_instruction_grouping_key = custom_instruction_grouping_key
        self.override_all_restrictions = override_all_restrictions
        self.advice = advice
        self.batch_details = Mock()
        self.client_batch_id = Mock()
        self.client_id = client_id
        self.value_timestamp = Mock()
        self.batch_id = Mock()
        self.insertion_timestamp = Mock()
        self.balances = Mock()


class PostingInstructionBatch(list):
    def __init__(
        self,
        batch_details=None,
        client_batch_id=None,
        value_timestamp=None,
        batch_id=None,
        client_id=None,
        posting_instructions=None,
        insertion_timestamp=None,
    ):
        super().__init__(posting_instructions)
        self.batch_details = batch_details
        self.client_batch_id = client_batch_id
        self.value_timestamp = value_timestamp
        self.batch_id = batch_id
        self.client_id = client_id
        self.posting_instructions = posting_instructions
        self.insertion_timestamp = insertion_timestamp

    def balances(exclude_advice):
        return BalanceDefaultDict()

    def __iter__(self):
        return iter(self.posting_instructions)

    def __next__(self):
        return next(self.posting_instructions)


class StringShape:
    pass


class UnionItem:
    def __init__(self, key=None, display_name=None):
        self.key = key if key else Mock()
        self.display_name = display_name if display_name else Mock()


class UnionItemValue:
    def __init__(self, key=None):
        self.key = key


class UnionShape:
    def __init__(self, *args):
        self.items = Mock()


class AddressDetails:
    def __init__(self, account_address=None, description=None, tags=None):
        self.account_address = account_address
        self.description = description
        self.tags = tags


class EventType:
    def __init__(self, name=None, scheduler_tag_ids=None):
        self.name = name
        self.scheduler_tag_ids = scheduler_tag_ids


class EventTypesGroup:
    def __init__(self, name=None, event_types_order=None):
        self.name = name
        self.event_types_order = event_types_order


class AddAccountNoteDirective:
    def __init__(
        self,
        idempotency_key=None,
        account_id=None,
        body=None,
        note_type=None,
        date=None,
        is_visible_to_customer=None,
    ):
        self.idempotency_key = idempotency_key
        self.account_id = account_id
        self.body = body
        self.note_type = note_type
        self.date = date
        self.is_visible_to_customer = is_visible_to_customer


class AmendScheduleDirective:
    def __init__(
        self, event_type=None, new_schedule=None, request_id=None, account_id=None
    ):
        self.event_type = event_type
        self.new_schedule = new_schedule
        self.request_id = request_id
        self.account_id = account_id


class HookDirectives:
    def __init__(
        self,
        add_account_note_directives=None,
        amend_schedule_directives=None,
        remove_schedules_directives=None,
        workflow_start_directives=None,
        posting_instruction_batch_directives=None,
    ):
        self.add_account_note_directives = add_account_note_directives
        self.amend_schedule_directives = amend_schedule_directives
        self.remove_schedules_directives = remove_schedules_directives
        self.workflow_start_directives = workflow_start_directives
        self.posting_instruction_batch_directives = posting_instruction_batch_directives


class PostingInstructionBatchDirective:
    def __init__(self, request_id=None, posting_instruction_batch=None):
        self.request_id = request_id
        self.posting_instruction_batch = posting_instruction_batch


class RemoveSchedulesDirective:
    def __init__(self, account_id=None, event_types=None, request_id=None):
        self.account_id = account_id
        self.event_types = event_types
        self.request_id = request_id


class WorkflowStartDirective:
    def __init__(
        self, workflow=None, context=None, account_id=None, idempotency_key=None
    ):
        self.workflow = workflow
        self.context = context
        self.account_id = account_id
        self.idempotency_key = idempotency_key


_ALL_TYPES = {
    "calendar": calendar,
    "DEFAULT_ASSET": DEFAULT_ASSET,
    "NamedTuple": typing.NamedTuple,
    "ROUND_05UP": decimal.ROUND_05UP,
    "Level": Level,
    "ROUND_CEILING": decimal.ROUND_CEILING,
    "NumberKind": NumberKind,
    "OptionalShape": OptionalShape,
    "Phase": Phase,
    "Balance": Balance,
    "AccountIdShape": AccountIdShape,
    "BalanceTimeseries": BalanceTimeseries,
    "PostingInstructionBatchDirective": PostingInstructionBatchDirective,
    "UnionShape": UnionShape,
    "Dict": typing.Dict,
    "NumberShape": NumberShape,
    "NewType": typing.NewType,
    "Iterable": typing.Iterable,
    "defaultdict": collections.defaultdict,
    "Iterator": typing.Iterator,
    "Set": typing.Set,
    "datetime": datetime.datetime,
    "DEFAULT_ADDRESS": DEFAULT_ADDRESS,
    "AddAccountNoteDirective": AddAccountNoteDirective,
    "PostingInstruction": PostingInstruction,
    "BalanceDefaultDict": BalanceDefaultDict,
    "FlagTimeseries": FlagTimeseries,
    "HookDirectives": HookDirectives,
    "Decimal": decimal.Decimal,
    "Union": typing.Union,
    "AmendScheduleDirective": AmendScheduleDirective,
    "PostingInstructionType": PostingInstructionType,
    "Type": typing.Type,
    "UnionItem": UnionItem,
    "AddressDetails": AddressDetails,
    "ROUND_FLOOR": decimal.ROUND_FLOOR,
    "Rejected": Rejected,
    "ClientTransactionEffects": ClientTransactionEffects,
    "ParameterTimeseries": ParameterTimeseries,
    "RemoveSchedulesDirective": RemoveSchedulesDirective,
    "RejectedReason": RejectedReason,
    "DateShape": DateShape,
    "json_dumps": json.dumps,
    "DenominationShape": DenominationShape,
    "UnionItemValue": UnionItemValue,
    "PostingInstructionBatch": PostingInstructionBatch,
    "parse_to_datetime": parser.parse,
    "ClientTransactionEffectsDefaultDict": ClientTransactionEffectsDefaultDict,
    "ROUND_HALF_DOWN": decimal.ROUND_HALF_DOWN,
    "Mapping": typing.Mapping,
    "json_loads": json.loads,
    "NoReturn": typing.NoReturn,
    "List": typing.List,
    "ROUND_DOWN": decimal.ROUND_DOWN,
    "Any": typing.Any,
    "NoteType": NoteType,
    "OptionalValue": OptionalValue,
    "math": math,
    "ROUND_HALF_UP": decimal.ROUND_HALF_UP,
    "Optional": typing.Optional,
    "ROUND_HALF_EVEN": decimal.ROUND_HALF_EVEN,
    "Tuple": typing.Tuple,
    "InvalidContractParameter": InvalidContractParameter,
    "Features": Features,
    "Tside": Tside,
    "UpdatePermission": UpdatePermission,
    "ClientTransaction": ClientTransaction,
    "Callable": typing.Callable,
    "EventType": EventType,
    "DefaultDict": typing.DefaultDict,
    "Parameter": Parameter,
    "StringShape": StringShape,
    "WorkflowStartDirective": WorkflowStartDirective,
    "TRANSACTION_REFERENCE_FIELD_NAME": TRANSACTION_REFERENCE_FIELD_NAME,
    "EventTypesGroup": EventTypesGroup,
    "timedelta": relativedelta.relativedelta,
}

_WHITELISTED_BUILTINS = {
    "all",
    "iter",
    "dir",
    "float",
    "tuple",
    "list",
    "min",
    "sorted",
    "max",
    "set",
    "complex",
    "id",
    "round",
    "dict",
    "bool",
    "reversed",
    "False",
    "None",
    "map",
    "int",
    "oct",
    "ord",
    "filter",
    "bin",
    "format",
    "hex",
    "frozenset",
    "range",
    "bytes",
    "any",
    "print",
    "abs",
    "bytearray",
    "repr",
    "len",
    "str",
    "True",
    "slice",
    "divmod",
    "hash",
    "next",
    "zip",
    "enumerate",
    "sum",
    "chr",
    "pow",
}

_SUPPORTED_HOOK_NAMES = {
    "pre_transaction_code",
    "post_transaction_code",
    "post_activate_code",
    "execution_schedules",
    "scheduled_code",
    "denomination_support_hint",
    "close_code",
    "upgrade_code",
    "post_parameter_change_code",
    "pre_parameter_change_code",
    "pre_posting_code",
    "post_posting_code",
    "derived_parameters",
}

# flake8: noqa
