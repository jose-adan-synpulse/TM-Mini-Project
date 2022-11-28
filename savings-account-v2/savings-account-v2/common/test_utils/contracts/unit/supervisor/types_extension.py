# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
from unittest.mock import Mock
from common.test_utils.contracts.unit.supervisor.types import *
from common.test_utils.contracts.unit.supervisor.types import (
    _ALL_TYPES,
    _WHITELISTED_BUILTINS,
    _SUPPORTED_HOOK_NAMES,
)
from common.test_utils.contracts.unit.types_extension import (
    Balance,
    BalanceTimeseries,
    NumberShape,
    OptionalValue,
    PostingInstruction,
    UnionItem,
    UnionShape,
    EventTypeSchedule,
    InvalidContractParameter,
    EndOfMonthSchedule,
)

# added as a workaround for TM-36940
class SmartContractDescriptorOverride:
    def __init__(
        self,
        alias=None,
        smart_contract_version_id=None,
        supervise_post_posting_hook=None,
    ):
        self.alias = alias
        self.smart_contract_version_id = smart_contract_version_id
        self.supervise_post_posting_hook = supervise_post_posting_hook


# All overridden types
_ALL_TYPES["Balance"] = Balance
_ALL_TYPES["BalanceTimeseries"] = BalanceTimeseries
_ALL_TYPES["NumberShape"] = NumberShape
_ALL_TYPES["OptionalValue"] = OptionalValue
_ALL_TYPES["PostingInstruction"] = PostingInstruction
_ALL_TYPES["UnionItem"] = UnionItem
_ALL_TYPES["UnionShape"] = UnionShape
_ALL_TYPES["SmartContractDescriptor"] = SmartContractDescriptorOverride
_ALL_TYPES["EventTypeSchedule"] = EventTypeSchedule
_ALL_TYPES["InvalidContractParameter"] = InvalidContractParameter
_ALL_TYPES["EndOfMonthSchedule"] = EndOfMonthSchedule


# flake8: noqa
