# standard libs
from datetime import datetime
from decimal import Decimal
from collections import namedtuple
from typing import Dict, DefaultDict, List, Tuple

DEFAULT_ADDRESS = "DEFAULT"
DEFAULT_ASSET = "COMMERCIAL_BANK_MONEY"
DEFAULT_DENOM = "GBP"

BalanceDimensions = namedtuple(
    "BalanceDimensions",
    ["address", "asset", "denomination", "phase"],
    defaults=[DEFAULT_ADDRESS, DEFAULT_ASSET, DEFAULT_DENOM, "POSTING_PHASE_COMMITTED"],
)


class Balance(object):
    net = Decimal("0")
    credit = Decimal("0")
    debit = Decimal("0")
    value_timestamp = None

    def __init__(
        self,
        credit: Decimal = Decimal("0"),
        debit: Decimal = Decimal("0"),
        net: Decimal = Decimal("0"),
        value_timestamp: datetime = None,
    ):
        self.credit = Decimal(credit)
        self.debit = Decimal(debit)
        self.net = Decimal(net)
        self.value_timestamp = value_timestamp

    def __repr__(self):
        return f"{self.net}"

    def __str__(self):
        return f"{self.net}"

    def __eq__(self, other):
        return (
            isinstance(other, Balance)
            and self.net == other.net
            and self.credit == other.credit
            and self.debit == other.debit
        )


class ExpectedBalanceComparison(Balance):

    actual_balance: Balance

    def __init__(
        self,
        credit: Decimal = Decimal("0"),
        debit: Decimal = Decimal("0"),
        net: Decimal = Decimal("0"),
    ):
        super().__init__(credit=credit, debit=debit, net=net)
        self.actual_balance = None

    def has_difference(self):
        return self != self.actual_balance

    def get_expected_and_actual_as_dict(self):

        return (
            {
                "expected": {
                    "net": self.net,
                    "credit": self.credit,
                    "debit": self.debit,
                },
                "actual": {
                    "net": self.actual_balance.net,
                    "credit": self.actual_balance.credit,
                    "debit": self.actual_balance.debit,
                },
                "timestamp": self.actual_balance.value_timestamp,
            }
            if self.actual_balance
            else None
        )

    def set_actual_balance(self, actual_balance):
        if type(actual_balance) == Balance:
            self.actual_balance = actual_balance
        else:
            raise ValueError(
                f"actual balance attribute type: {type(actual_balance)}, expect Balance."
            )


def compare_balances(
    expected_balances: List[Tuple[BalanceDimensions, str]],
    actual_balances: DefaultDict[BalanceDimensions, Balance],
) -> Dict[BalanceDimensions, Dict[str, Decimal]]:

    """
    Compare two sets of balances, returning a dictionary with dimensions for which the two did not
    match as keys, and a dictionary containing the expected and actual balances for those
    dimensions as the values. For example:
    {
        BalanceDimensions('DEFAULT', 'COMMERCIAL_BANK_MONEY', 'GBP', 'POSTING_PHASE_COMMITTED'):{
            'expected': Decimal('10'),
            'actual': Decimal('5')
        }
    }

    Able to compare credits and debits of a balance if compare_debits_and_credits is True
    - Note that to be able to do this the expected balances must be
    Dict[BalanceDimensions, ExpectedBalanceComparison]
    """
    return {
        dimensions: {
            "expected": Decimal(net),
            "actual": actual_balances[dimensions].net,
        }
        for dimensions, net in expected_balances
        if Decimal(net) != actual_balances[dimensions].net
    }
