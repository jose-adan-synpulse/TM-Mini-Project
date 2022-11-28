# Copyright @ 2020-2021 Thought Machine Group Limited. All rights reserved.
# standard libs
from typing import Dict, List, Set
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class AccountsPostingsProducerResults:
    accounts_loaded: List[str] = field(default_factory=list)
    accounts_failed: List[str] = field(default_factory=list)


@dataclass
class AccountPostingsInProgress:
    last_updated: datetime
    posting_request_id: str = None
    pib_id_in_progress: str = ""
    pib_status_in_progress: str = ""
    postings_sent: int = 0
    postings_completed: int = 0


@dataclass
class ExpectedWorklowInstantiation:
    workflow_definition_id: str
    count: int
    request_ids: Set[str] = field(default_factory=set)

    def __eq__(self, other):
        return (
            type(other) == ExpectedWorklowInstantiation
            and self.workflow_definition_id == other.workflow_definition_id
            and self.count == other.count
        )


@dataclass
class ExpectedOutcomeValidationResults:
    accounts_with_incorrect_balances: Dict[str, None] = field(default_factory=dict)
    accounts_with_missing_workflows: Dict[
        str, List[ExpectedWorklowInstantiation]
    ] = field(default_factory=dict)
