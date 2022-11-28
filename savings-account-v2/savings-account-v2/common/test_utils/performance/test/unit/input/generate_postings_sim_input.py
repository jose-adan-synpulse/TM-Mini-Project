from common.test_utils.contracts.simulation.data_objects.data_objects import (
    SimulationEvent,
)
import datetime


EVENTS = [
    SimulationEvent(
        time=datetime.datetime(2020, 1, 10, 9, 0, tzinfo=datetime.timezone.utc),
        event={
            "create_account": {
                "id": "1",
                "product_version_id": "1",
                "permitted_denominations": [],
                "status": "ACCOUNT_STATUS_UNKNOWN",
                "stakeholder_ids": [],
                "instance_param_vals": {},
                "derived_instance_param_vals": {},
                "details": {},
            }
        },
    ),
    SimulationEvent(
        time=datetime.datetime(2020, 1, 10, 9, 0, tzinfo=datetime.timezone.utc),
        event={
            "create_account": {
                "id": "Savings pot",
                "product_version_id": "1",
                "permitted_denominations": [],
                "status": "ACCOUNT_STATUS_UNKNOWN",
                "stakeholder_ids": [],
                "instance_param_vals": {},
                "derived_instance_param_vals": {},
                "details": {},
            }
        },
    ),
    SimulationEvent(
        time=datetime.datetime(2020, 1, 10, 9, 0, tzinfo=datetime.timezone.utc),
        event={
            "create_account": {
                "id": "Main account",
                "product_version_id": "2",
                "permitted_denominations": [],
                "status": "ACCOUNT_STATUS_UNKNOWN",
                "stakeholder_ids": [],
                "instance_param_vals": {
                    "arranged_overdraft_limit": "1000",
                    "unarranged_overdraft_limit": "2000",
                    "interest_application_day": "10",
                    "daily_atm_withdrawal_limit": "1000",
                },
                "derived_instance_param_vals": {},
                "details": {},
            }
        },
    ),
    SimulationEvent(
        time=datetime.datetime(2020, 1, 10, 9, 0, tzinfo=datetime.timezone.utc),
        event={
            "create_posting_instruction_batch": {
                "client_id": "AsyncCreatePostingInstructionBatch",
                "client_batch_id": "123",
                "posting_instructions": [
                    {
                        "client_transaction_id": "123456",
                        "instruction_details": {},
                        "override": {},
                        "inbound_hard_settlement": {
                            "amount": "50",
                            "denomination": "GBP",
                            "target_account": {"account_id": "Main account"},
                            "internal_account_id": "1",
                            "advice": False,
                        },
                    }
                ],
                "batch_details": {},
                "value_timestamp": "2020-01-10T09:00:00+00:00",
            }
        },
    ),
    SimulationEvent(
        time=datetime.datetime(2020, 1, 11, 9, 0, tzinfo=datetime.timezone.utc),
        event={
            "create_posting_instruction_batch": {
                "client_id": "AsyncCreatePostingInstructionBatch",
                "client_batch_id": "123",
                "posting_instructions": [
                    {
                        "client_transaction_id": "1",
                        "instruction_details": {},
                        "override": {},
                        "inbound_hard_settlement": {
                            "amount": "10",
                            "denomination": "GBP",
                            "target_account": {"account_id": "Main account"},
                            "internal_account_id": "1",
                            "advice": False,
                        },
                    }
                ],
                "batch_details": {},
                "value_timestamp": "2020-01-11T09:00:00+00:00",
            }
        },
    ),
]
