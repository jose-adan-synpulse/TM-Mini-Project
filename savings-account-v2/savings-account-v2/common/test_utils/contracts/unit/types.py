import decimal
import typing
import dateutil.parser
from unittest.mock import Mock
import datetime
import math
import collections
from enum import Enum
import dateutil.relativedelta
import json
import calendar

# Fixed Types
DEFAULT_ADDRESS = "DEFAULT"
DEFAULT_ASSET = "COMMERCIAL_BANK_MONEY"
TRANSACTION_REFERENCE_FIELD_NAME = "description"


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


class UpdatePermission(Enum):
    FIXED = "1"
    OPS_EDITABLE = "2"
    USER_EDITABLE = "3"
    USER_EDITABLE_WITH_OPS_PERMISSION = "4"


class DefinedDateTime(Enum):
    EFFECTIVE_TIME = "1"
    INTERVAL_START = "2"
    LIVE = "-1"


class ScheduleFailover(Enum):
    FIRST_VALID_DAY_AFTER = "2"
    FIRST_VALID_DAY_BEFORE = "1"


class AccountIdShape:
    pass


class Balance:
    def __init__(self, credit=None, debit=None, net=None):
        self.credit = credit
        self.debit = debit
        self.net = net


class BalanceDefaultDict(dict):
    pass


class BalanceTimeseries(list):
    def __init__(self, iterable=None):
        super().__init__(iterable)
        self.iterable = iterable
        self.at = Mock()
        self.before = Mock()
        self.latest = Mock()
        self.all = Mock()


class ClientTransaction(list):
    def __init__(self, posting_instructions=None):
        super().__init__(posting_instructions)
        self.is_custom = Mock()
        self.cancelled = Mock()
        self.start_time = Mock()
        self.effects = Mock()
        self.balances = Mock()


class ClientTransactionEffects:
    def __init__(self, authorised=None, released=None, settled=None, unsettled=None):
        self.authorised = authorised
        self.released = released
        self.settled = settled
        self.unsettled = unsettled


class ClientTransactionEffectsDefaultDict(dict):
    pass


class DateShape:
    def __init__(self, min_date=None, max_date=None):
        self.min_date = min_date
        self.max_date = max_date


class DenominationShape:
    def __init__(self, permitted_denominations=None):
        self.permitted_denominations = permitted_denominations


class FlagTimeseries(list):
    def __init__(self, iterable=None):
        super().__init__(iterable)
        self.iterable = iterable
        self.at = Mock()
        self.before = Mock()
        self.latest = Mock()
        self.all = Mock()


class NumberShape:
    def __init__(self, kind=None, min_value=None, max_value=None, step=None):
        self.kind = kind
        self.min_value = min_value
        self.max_value = max_value
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


class ParameterTimeseries(list):
    def __init__(self, iterable=None):
        super().__init__(iterable)
        self.iterable = iterable
        self.at = Mock()
        self.before = Mock()
        self.latest = Mock()
        self.all = Mock()


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
        advice=None,
        transaction_code=None,
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
        self.transaction_code = transaction_code
        self.batch_details = Mock()
        self.client_batch_id = Mock()
        self.client_id = Mock()
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
        self.balances = Mock()


class StringShape:
    pass


class UnionItem:
    def __init__(self, key=None, display_name=None):
        self.key = key
        self.display_name = display_name


class UnionItemValue:
    def __init__(self, key=None):
        self.key = key


class UnionShape:
    def __init__(self, items=None):
        self.items = items


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
        update_account_event_type_directives=None,
    ):
        self.add_account_note_directives = add_account_note_directives
        self.amend_schedule_directives = amend_schedule_directives
        self.remove_schedules_directives = remove_schedules_directives
        self.workflow_start_directives = workflow_start_directives
        self.posting_instruction_batch_directives = posting_instruction_batch_directives
        self.update_account_event_type_directives = update_account_event_type_directives


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


class CalendarEvent:
    def __init__(
        self, id=None, calendar_id=None, start_timestamp=None, end_timestamp=None
    ):
        self.id = id
        self.calendar_id = calendar_id
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp


class CalendarEvents(list):
    def __init__(self, calendar_events=None):
        self.calendar_events = calendar_events


class TransactionCode:
    def __init__(self, domain=None, family=None, subfamily=None):
        self.domain = domain
        self.family = family
        self.subfamily = subfamily


class EventTypeSchedule:
    def __init__(
        self,
        day=None,
        day_of_week=None,
        hour=None,
        minute=None,
        second=None,
        month=None,
        year=None,
    ):
        self.day = day
        self.day_of_week = day_of_week
        self.hour = hour
        self.minute = minute
        self.second = second
        self.month = month
        self.year = year


class UpdateAccountEventTypeDirective:
    def __init__(
        self,
        account_id=None,
        event_type=None,
        schedule=None,
        end_datetime=None,
        schedule_method=None,
    ):
        self.account_id = account_id
        self.event_type = event_type
        self.schedule = schedule
        self.end_datetime = end_datetime
        self.schedule_method = schedule_method


class ContractModule:
    def __init__(self, alias=None, expected_interface=None):
        self.alias = alias
        self.expected_interface = expected_interface


class SharedFunction:
    def __init__(self, name=None, args=None, return_type=None):
        self.name = name
        self.args = args
        self.return_type = return_type


class SharedFunctionArg:
    def __init__(self, name=None, type=None):
        self.name = name
        self.type = type


class BalancesFilter:
    def __init__(self, addresses=None):
        self.addresses = addresses


class BalancesIntervalFetcher:
    def __init__(self, fetcher_id=None, start=None, end=None, filter=None):
        self.fetcher_id = fetcher_id
        self.start = start
        self.end = end
        self.filter = filter


class BalancesObservation:
    def __init__(self, value_datetime=None, balances=None):
        self.value_datetime = value_datetime
        self.balances = balances


class BalancesObservationFetcher:
    def __init__(self, fetcher_id=None, at=None, filter=None):
        self.fetcher_id = fetcher_id
        self.at = at
        self.filter = filter


class RelativeDateTime:
    def __init__(self, shift=None, find=None, origin=None):
        self.shift = shift
        self.find = find
        self.origin = origin


class Next:
    def __init__(self, month=None, day=None, hour=None, minute=None, second=None):
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second


class Override:
    def __init__(
        self, year=None, month=None, day=None, hour=None, minute=None, second=None
    ):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second


class PostingsIntervalFetcher:
    def __init__(self, fetcher_id=None, start=None, end=None):
        self.fetcher_id = fetcher_id
        self.start = start
        self.end = end


class Previous:
    def __init__(self, month=None, day=None, hour=None, minute=None, second=None):
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second


class Shift:
    def __init__(
        self, years=None, months=None, days=None, hours=None, minutes=None, seconds=None
    ):
        self.years = years
        self.months = months
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds


class ScheduledJob:
    def __init__(self, pause_datetime=None):
        self.pause_datetime = pause_datetime


class EndOfMonthSchedule:
    def __init__(self, day=None, hour=None, minute=None, second=None, failover=None):
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second
        self.failover = failover


_ALL_TYPES = {
    "Level": Level,
    "Balance": Balance,
    "PostingInstruction": PostingInstruction,
    "ROUND_CEILING": decimal.ROUND_CEILING,
    "PostingInstructionBatchDirective": PostingInstructionBatchDirective,
    "BalancesFilter": BalancesFilter,
    "Shift": Shift,
    "Dict": typing.Dict,
    "NewType": typing.NewType,
    "defaultdict": collections.defaultdict,
    "Set": typing.Set,
    "datetime": datetime.datetime,
    "BalanceDefaultDict": BalanceDefaultDict,
    "BalancesObservationFetcher": BalancesObservationFetcher,
    "AddressDetails": AddressDetails,
    "Type": typing.Type,
    "TransactionCode": TransactionCode,
    "ScheduledJob": ScheduledJob,
    "RejectedReason": RejectedReason,
    "DenominationShape": DenominationShape,
    "UnionItemValue": UnionItemValue,
    "SharedFunctionArg": SharedFunctionArg,
    "PostingInstructionBatch": PostingInstructionBatch,
    "ClientTransactionEffectsDefaultDict": ClientTransactionEffectsDefaultDict,
    "UpdateAccountEventTypeDirective": UpdateAccountEventTypeDirective,
    "List": typing.List,
    "ROUND_DOWN": decimal.ROUND_DOWN,
    "Any": typing.Any,
    "SharedFunction": SharedFunction,
    "ROUND_HALF_UP": decimal.ROUND_HALF_UP,
    "Features": Features,
    "Tside": Tside,
    "UpdatePermission": UpdatePermission,
    "ClientTransaction": ClientTransaction,
    "AddAccountNoteDirective": AddAccountNoteDirective,
    "EventTypeSchedule": EventTypeSchedule,
    "EventType": EventType,
    "ContractModule": ContractModule,
    "Parameter": Parameter,
    "Callable": typing.Callable,
    "CalendarEvent": CalendarEvent,
    "DefaultDict": typing.DefaultDict,
    "PostingsIntervalFetcher": PostingsIntervalFetcher,
    "PostingInstructionType": PostingInstructionType,
    "TRANSACTION_REFERENCE_FIELD_NAME": TRANSACTION_REFERENCE_FIELD_NAME,
    "OptionalValue": OptionalValue,
    "timedelta": dateutil.relativedelta.relativedelta,
    "AmendScheduleDirective": AmendScheduleDirective,
    "EndOfMonthSchedule": EndOfMonthSchedule,
    "BalancesObservation": BalancesObservation,
    "NumberShape": NumberShape,
    "calendar": calendar,
    "DEFAULT_ASSET": DEFAULT_ASSET,
    "NamedTuple": typing.NamedTuple,
    "ROUND_05UP": decimal.ROUND_05UP,
    "ScheduleFailover": ScheduleFailover,
    "NumberKind": NumberKind,
    "OptionalShape": OptionalShape,
    "AccountIdShape": AccountIdShape,
    "Phase": Phase,
    "BalanceTimeseries": BalanceTimeseries,
    "CalendarEvents": CalendarEvents,
    "RelativeDateTime": RelativeDateTime,
    "UnionShape": UnionShape,
    "Previous": Previous,
    "Iterable": typing.Iterable,
    "Iterator": typing.Iterator,
    "FlagTimeseries": FlagTimeseries,
    "HookDirectives": HookDirectives,
    "Decimal": decimal.Decimal,
    "Union": typing.Union,
    "DefinedDateTime": DefinedDateTime,
    "UnionItem": UnionItem,
    "ROUND_FLOOR": decimal.ROUND_FLOOR,
    "Rejected": Rejected,
    "ClientTransactionEffects": ClientTransactionEffects,
    "ParameterTimeseries": ParameterTimeseries,
    "RemoveSchedulesDirective": RemoveSchedulesDirective,
    "DateShape": DateShape,
    "Next": Next,
    "json_dumps": json.dumps,
    "parse_to_datetime": dateutil.parser.parse,
    "ROUND_HALF_DOWN": decimal.ROUND_HALF_DOWN,
    "Mapping": typing.Mapping,
    "json_loads": json.loads,
    "NoReturn": typing.NoReturn,
    "NoteType": NoteType,
    "Override": Override,
    "math": math,
    "Optional": typing.Optional,
    "Tuple": typing.Tuple,
    "ROUND_HALF_EVEN": decimal.ROUND_HALF_EVEN,
    "InvalidContractParameter": InvalidContractParameter,
    "BalancesIntervalFetcher": BalancesIntervalFetcher,
    "EventTypesGroup": EventTypesGroup,
    "StringShape": StringShape,
    "WorkflowStartDirective": WorkflowStartDirective,
    "DEFAULT_ADDRESS": DEFAULT_ADDRESS,
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
    "post_activate_code",
    "execution_schedules",
    "scheduled_code",
    "close_code",
    "upgrade_code",
    "post_parameter_change_code",
    "pre_parameter_change_code",
    "pre_posting_code",
    "post_posting_code",
    "derived_parameters",
}

# flake8: noqa