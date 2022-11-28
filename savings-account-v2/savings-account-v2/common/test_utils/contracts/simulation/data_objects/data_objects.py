# Copyright @ 2020-2021 Thought Machine Group Limited. All rights reserved.
from typing import Dict, List
from datetime import datetime
from dataclasses import dataclass, field


@dataclass
class SimulationEvent:
    time: datetime
    event: Dict


@dataclass
class AccountConfig:
    instance_params: Dict
    account_id_base: str = "Main account"
    number_of_accounts: int = 1


@dataclass
class ContractModuleConfig:
    alias: str
    file_path: str
    version_id: str = None


@dataclass
class ContractConfig:
    contract_file_path: str
    template_params: Dict
    account_configs: List[AccountConfig]
    alias: str = None
    smart_contract_version_id: str = "0"
    linked_contract_modules: List[ContractModuleConfig] = None


@dataclass
class SupervisorConfig:
    supervisor_file_path: str
    supervisee_contracts: List[ContractConfig]
    plan_id: str = "1"
    supervisor_contract_version_id: str = "supervisor version 1"
    associate_supervisees_to_plan: bool = True


@dataclass
class SuperviseeConfig:
    contract_id: str
    contract_file: str
    account_name: str
    version: str
    instance_parameters: Dict
    template_parameters: Dict
    instances: int = 1
    linked_contract_modules: List[ContractModuleConfig] = None


@dataclass
class ExpectedRejection:
    timestamp: datetime
    rejection_type: str
    rejection_reason: str
    account_id: str = "Main account"


@dataclass
class ExpectedSchedule:
    run_times: List[datetime]
    event_id: str
    account_id: str = None
    plan_id: str = None
    count: int = None


@dataclass
class ExpectedWorkflow:
    workflow_definition_id: str
    account_id: str = "Main account"
    count: int = None
    run_times: List[datetime] = field(default_factory=list)
    contexts: List[Dict] = field(default_factory=list)


@dataclass
class ExpectedDerivedParameter:
    timestamp: datetime
    account_id: str = "Main account"
    name: str = None
    value: str = None


@dataclass
class SubTest:
    description: str
    expected_balances_at_ts: Dict = None
    expected_schedules: List[ExpectedSchedule] = None
    expected_posting_rejections: List[ExpectedRejection] = None
    expected_workflows: List[ExpectedWorkflow] = None
    expected_derived_parameters: List[ExpectedDerivedParameter] = None
    events: List[SimulationEvent] = None


@dataclass
class SimulationTestScenario:
    sub_tests: List[SubTest]
    start: datetime
    end: datetime = None
    contract_config: ContractConfig = None
    supervisor_config: SupervisorConfig = None
    internal_accounts: Dict = None
    debug: bool = False
