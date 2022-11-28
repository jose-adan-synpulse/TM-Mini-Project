from collections import defaultdict
from copy import deepcopy
from common.test_utils.contracts.unit.types import *
from decimal import Decimal
from unittest.mock import Mock
from .types import _ALL_TYPES, _WHITELISTED_BUILTINS, _SUPPORTED_HOOK_NAMES


class Balance(Balance):
    def __init__(self, credit=Decimal(0), debit=Decimal(0), net=Decimal(0)):
        super().__init__(credit, debit, net)

    def _adjust(self, credit=None, debit=None, net=None):
        if credit:
            self.credit += credit
        if debit:
            self.debit += debit
        if net:
            self.net += net

    def __add__(self, other):
        return self.__class__(
            credit=self.credit + other.credit,
            debit=self.debit + other.debit,
            net=self.net + other.net,
        )

    def __radd__(self, other):
        return self.__add__(other)

    def __iadd__(self, other):
        self.credit += other.credit
        self.debit += other.debit
        self.net += other.net
        return self

    def __repr__(self):
        return f"{self.net}"

    def __str__(self):
        return f"{self.net}"


class BalanceDefaultDict(defaultdict):
    _balance = Balance

    def __init__(self, *args, **kwargs):
        super(BalanceDefaultDict, self).__init__(*args, **kwargs)

    def __add__(self, other):
        aggregated_balance_dict = deepcopy(self)
        for balance_key, balance in other.items():
            if balance_key in aggregated_balance_dict:
                aggregated_balance_dict[balance_key] = (
                    aggregated_balance_dict[balance_key] + balance
                )
            else:
                aggregated_balance_dict[balance_key] = balance

        return aggregated_balance_dict

    def __radd__(self, other):
        return self.__add__(other)

    def __iadd__(self, other):
        for balance_key, balance in other.items():
            self[balance_key] = self.get(balance_key, Balance()) + balance
        return self


class BalanceTimeseries(BalanceTimeseries):
    def __init__(self, iterable=None):
        super().__init__(iterable)
        self.latest = Balance()


class NumberShape(NumberShape):
    def __call__(self, kind=None, min_value=None, max_value=None, step=None):
        if kind:
            self.kind = kind
        if min_value:
            self.min_value = min_value
        if max_value:
            self.max_value = max_value
        if step:
            self.step = step


class OptionalValue(OptionalValue):
    def __init__(self, value=None, is_set=True):
        super().__init__(value)
        self.is_set = Mock(return_value=is_set)


class PostingInstruction(PostingInstruction):
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
        value_timestamp=None,
    ):
        super().__init__(
            account_address,
            account_id,
            amount,
            asset,
            credit,
            denomination,
            final,
            phase,
            id,
            type,
            client_transaction_id,
            instruction_details,
            pics,
            custom_instruction_grouping_key,
            override_all_restrictions,
            advice,
        )
        self.client_id = client_id
        self.value_timestamp = value_timestamp


class UnionItem(UnionItem):
    def __init__(self, key=None, display_name=None):
        super().__init__(key, display_name)
        self.key = key if key else Mock()
        self.display_name = display_name if display_name else Mock()


class UnionShape(UnionShape):
    def __init__(self, *args):
        super().__init__(items=args)


# added as a workaround for TM-36940
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

    def __eq__(self, other):
        if not isinstance(other, EventTypeSchedule):
            return NotImplemented

        return (
            self.day == other.day
            and self.day_of_week == other.day_of_week
            and self.hour == other.hour
            and self.minute == other.minute
            and self.second == other.second
            and self.month == other.month
            and self.year == other.year
        )


class EndOfMonthSchedule(EndOfMonthSchedule):
    def __eq__(self, other) -> bool:
        if not isinstance(other, EndOfMonthSchedule):
            return NotImplemented

        return (
            self.day == other.day
            and self.hour == other.hour
            and self.minute == other.minute
            and self.second == other.second
            and self.failover == other.failover
        )


# All overridden types
_ALL_TYPES["Balance"] = Balance
_ALL_TYPES["BalanceTimeseries"] = BalanceTimeseries
_ALL_TYPES["NumberShape"] = NumberShape
_ALL_TYPES["OptionalValue"] = OptionalValue
_ALL_TYPES["PostingInstruction"] = PostingInstruction
_ALL_TYPES["UnionItem"] = UnionItem
_ALL_TYPES["UnionShape"] = UnionShape
_ALL_TYPES["EventTypeSchedule"] = EventTypeSchedule
_ALL_TYPES["EndOfMonthSchedule"] = EndOfMonthSchedule


# flake8: noqa
