SIM_RESULT = [
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": [
                "created smart contract module versions link with id "
                '"sim_link_utils_module_with_contract_2"'
            ],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": ['created account "1"'],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": ['created account "Savings pot"'],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": [
                'created account "Main account"',
                'set account parameter "arranged_overdraft_limit" value to "1000"',
                'set account parameter "unarranged_overdraft_limit" value to "2000"',
                'set account parameter "interest_application_day" value to "10"',
                'set account parameter "daily_atm_withdrawal_limit" value to "1000"',
            ],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": ['created 1 scheduled event jobs for account "Main account"'],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": [],
            "posting_instruction_batches": [
                {
                    "id": "841a3dac-b22c-4ef8-885a-6b883d738763",
                    "create_request_id": "",
                    "client_id": "AsyncCreatePostingInstructionBatch",
                    "client_batch_id": "123",
                    "posting_instructions": [
                        {
                            "id": "9f7beab1-850e-4c00-a9cf-e27f00e540f3",
                            "client_transaction_id": "123456",
                            "inbound_hard_settlement": {
                                "amount": "50",
                                "denomination": "GBP",
                                "target_account": {"account_id": "Main account"},
                                "internal_account_id": "1",
                                "advice": False,
                                "target_account_id": "Main account",
                            },
                            "pics": [],
                            "instruction_details": {},
                            "committed_postings": [
                                {
                                    "credit": True,
                                    "amount": "50",
                                    "denomination": "GBP",
                                    "account_id": "Main account",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                                {
                                    "credit": False,
                                    "amount": "50",
                                    "denomination": "GBP",
                                    "account_id": "1",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                            ],
                            "posting_violations": [],
                            "account_violations": [],
                            "restriction_violations": [],
                            "contract_violations": [],
                            "override": {"restrictions": None},
                            "transaction_code": None,
                        }
                    ],
                    "batch_details": {},
                    "value_timestamp": "2020-01-10T09:00:00Z",
                    "status": "POSTING_INSTRUCTION_BATCH_STATUS_UNKNOWN",
                    "error": None,
                    "insertion_timestamp": "2020-01-10T09:00:00Z",
                    "dry_run": False,
                }
            ],
            "balances": {
                "1": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "1",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-10T09:00:00Z",
                            "amount": "-50",
                            "total_debit": "50",
                            "total_credit": "0",
                        }
                    ]
                },
                "Main account": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-10T09:00:00Z",
                            "amount": "50",
                            "total_debit": "0",
                            "total_credit": "50",
                        }
                    ]
                },
            },
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-11T01:02:03Z",
            "logs": [
                'processed scheduled event "ACCRUE_INTEREST_AND_DAILY_FEES" for account '
                '"Main account"'
            ],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-11T01:02:03Z",
            "logs": [],
            "posting_instruction_batches": [
                {
                    "id": "0464e00f-c51d-493f-ac9e-ab8e10ce45b6",
                    "create_request_id": "",
                    "client_id": "CoreContracts",
                    "client_batch_id": "ACCRUE_INTEREST_AND_DAILY_FEES_Main account_5_ACCRUE_"
                    "INTEREST_AND_DAILY_FEES_1578704523000000000",
                    "posting_instructions": [
                        {
                            "id": "4c4013f3-e8aa-4c25-a245-dd0c4fba1170",
                            "client_transaction_id": "ACCRUE_DEPOSIT_INTEREST_TIER1_Main account"
                            "_5_ACCRUE_INTEREST_AND_DAILY_FEES_1578704523000000000_GBP_INTERNAL",
                            "custom_instruction": {
                                "postings": [
                                    {
                                        "credit": False,
                                        "amount": "0.00685",
                                        "denomination": "GBP",
                                        "account_id": "1",
                                        "account_address": "DEFAULT",
                                        "asset": "COMMERCIAL_BANK_MONEY",
                                        "phase": "POSTING_PHASE_COMMITTED",
                                    },
                                    {
                                        "credit": True,
                                        "amount": "0.00685",
                                        "denomination": "GBP",
                                        "account_id": "1",
                                        "account_address": "DEFAULT",
                                        "asset": "COMMERCIAL_BANK_MONEY",
                                        "phase": "POSTING_PHASE_COMMITTED",
                                    },
                                ]
                            },
                            "pics": [],
                            "instruction_details": {
                                "description": "Daily deposit interest accrued at 0.01370% on "
                                "balance of 50.00",
                                "event": "ACCRUE_INTEREST_AND_DAILY_FEES",
                                "originating_account_id": "Main account",
                            },
                            "committed_postings": [
                                {
                                    "credit": False,
                                    "amount": "0.00685",
                                    "denomination": "GBP",
                                    "account_id": "1",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                                {
                                    "credit": True,
                                    "amount": "0.00685",
                                    "denomination": "GBP",
                                    "account_id": "1",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                            ],
                            "posting_violations": [],
                            "account_violations": [],
                            "restriction_violations": [],
                            "contract_violations": [],
                            "override": {
                                "restrictions": {"all": True, "restriction_set_ids": []}
                            },
                            "transaction_code": None,
                        },
                        {
                            "id": "496a5fa7-24ad-4381-be69-21b578d9d08b",
                            "client_transaction_id": "ACCRUE_DEPOSIT_INTEREST_TIER1_Main account_5_"
                            "ACCRUE_INTEREST_AND_DAILY_FEES_1578704523000000000_GBP_CUSTOMER",
                            "custom_instruction": {
                                "postings": [
                                    {
                                        "credit": False,
                                        "amount": "0.00685",
                                        "denomination": "GBP",
                                        "account_id": "Main account",
                                        "account_address": "INTERNAL_CONTRA",
                                        "asset": "COMMERCIAL_BANK_MONEY",
                                        "phase": "POSTING_PHASE_COMMITTED",
                                    },
                                    {
                                        "credit": True,
                                        "amount": "0.00685",
                                        "denomination": "GBP",
                                        "account_id": "Main account",
                                        "account_address": "ACCRUED_DEPOSIT",
                                        "asset": "COMMERCIAL_BANK_MONEY",
                                        "phase": "POSTING_PHASE_COMMITTED",
                                    },
                                ]
                            },
                            "pics": [],
                            "instruction_details": {
                                "description": "Daily deposit interest accrued at 0.01370% on "
                                "balance of 50.00",
                                "event": "ACCRUE_INTEREST_AND_DAILY_FEES",
                                "originating_account_id": "Main account",
                            },
                            "committed_postings": [
                                {
                                    "credit": False,
                                    "amount": "0.00685",
                                    "denomination": "GBP",
                                    "account_id": "Main account",
                                    "account_address": "INTERNAL_CONTRA",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                                {
                                    "credit": True,
                                    "amount": "0.00685",
                                    "denomination": "GBP",
                                    "account_id": "Main account",
                                    "account_address": "ACCRUED_DEPOSIT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                            ],
                            "posting_violations": [],
                            "account_violations": [],
                            "restriction_violations": [],
                            "contract_violations": [],
                            "override": {
                                "restrictions": {"all": True, "restriction_set_ids": []}
                            },
                            "transaction_code": None,
                        },
                    ],
                    "batch_details": {},
                    "value_timestamp": "2020-01-11T01:02:02.999999Z",
                    "status": "POSTING_INSTRUCTION_BATCH_STATUS_UNKNOWN",
                    "error": None,
                    "insertion_timestamp": "2020-01-11T01:02:02.999999Z",
                    "dry_run": False,
                }
            ],
            "balances": {
                "1": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "1",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T01:02:02.999999Z",
                            "amount": "-50",
                            "total_debit": "50.00685",
                            "total_credit": "0.00685",
                        }
                    ]
                },
                "Main account": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "INTERNAL_CONTRA",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T01:02:02.999999Z",
                            "amount": "-0.00685",
                            "total_debit": "0.00685",
                            "total_credit": "0",
                        },
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "ACCRUED_DEPOSIT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T01:02:02.999999Z",
                            "amount": "0.00685",
                            "total_debit": "0",
                            "total_credit": "0.00685",
                        },
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-10T09:00:00Z",
                            "amount": "50",
                            "total_debit": "0",
                            "total_credit": "50",
                        },
                    ]
                },
            },
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-11T09:00:00Z",
            "logs": [],
            "posting_instruction_batches": [
                {
                    "id": "e28d6568-bf3c-4783-bc10-55eb5b9055c9",
                    "create_request_id": "",
                    "client_id": "AsyncCreatePostingInstructionBatch",
                    "client_batch_id": "123",
                    "posting_instructions": [
                        {
                            "id": "9133a7d3-1170-43fa-8cd3-c30185f3df22",
                            "client_transaction_id": "1",
                            "inbound_hard_settlement": {
                                "amount": "10",
                                "denomination": "GBP",
                                "target_account": {"account_id": "Main account"},
                                "internal_account_id": "1",
                                "advice": False,
                                "target_account_id": "Main account",
                            },
                            "pics": [],
                            "instruction_details": {},
                            "committed_postings": [
                                {
                                    "credit": True,
                                    "amount": "10",
                                    "denomination": "GBP",
                                    "account_id": "Main account",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                                {
                                    "credit": False,
                                    "amount": "10",
                                    "denomination": "GBP",
                                    "account_id": "1",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                            ],
                            "posting_violations": [],
                            "account_violations": [],
                            "restriction_violations": [],
                            "contract_violations": [],
                            "override": {"restrictions": None},
                            "transaction_code": None,
                        }
                    ],
                    "batch_details": {},
                    "value_timestamp": "2020-01-11T09:00:00Z",
                    "status": "POSTING_INSTRUCTION_BATCH_STATUS_UNKNOWN",
                    "error": None,
                    "insertion_timestamp": "2020-01-11T09:00:00Z",
                    "dry_run": False,
                }
            ],
            "balances": {
                "1": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "1",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T09:00:00Z",
                            "amount": "-60",
                            "total_debit": "60.00685",
                            "total_credit": "0.00685",
                        }
                    ]
                },
                "Main account": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T09:00:00Z",
                            "amount": "60",
                            "total_debit": "0",
                            "total_credit": "60",
                        },
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "INTERNAL_CONTRA",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T01:02:02.999999Z",
                            "amount": "-0.00685",
                            "total_debit": "0.00685",
                            "total_credit": "0",
                        },
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "ACCRUED_DEPOSIT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T01:02:02.999999Z",
                            "amount": "0.00685",
                            "total_debit": "0",
                            "total_credit": "0.00685",
                        },
                    ]
                },
            },
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
]

SIM_RESULT_CURRENT_ACCOUNT = [
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": ['created account "1"'],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": ['created account "Savings pot"'],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": [
                'created account "Main account"',
                'set account parameter "interest_application_day" value to "10"',
                'set account parameter "daily_atm_withdrawal_limit" value to "1000"',
                'set account parameter "arranged_overdraft_limit" value to "1000"',
                'set account parameter "unarranged_overdraft_limit" value to "2000"',
            ],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": ['created 1 scheduled event jobs for account "Main account"'],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-10T09:00:00Z",
            "logs": [],
            "posting_instruction_batches": [
                {
                    "id": "d1a84a74-8252-475c-b914-0efdf142f40d",
                    "create_request_id": "",
                    "client_id": "AsyncCreatePostingInstructionBatch",
                    "client_batch_id": "123",
                    "posting_instructions": [
                        {
                            "id": "ab990b62-0519-48a6-b09e-2636e3fdf0de",
                            "client_transaction_id": "123456",
                            "inbound_hard_settlement": {
                                "amount": "50",
                                "denomination": "GBP",
                                "target_account": {"account_id": "Main account"},
                                "internal_account_id": "1",
                                "advice": False,
                                "target_account_id": "Main account",
                            },
                            "pics": [],
                            "instruction_details": {},
                            "committed_postings": [
                                {
                                    "credit": True,
                                    "amount": "50",
                                    "denomination": "GBP",
                                    "account_id": "Main account",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                                {
                                    "credit": False,
                                    "amount": "50",
                                    "denomination": "GBP",
                                    "account_id": "1",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                            ],
                            "posting_violations": [],
                            "account_violations": [],
                            "restriction_violations": [],
                            "contract_violations": [],
                            "override": {"restrictions": None},
                            "transaction_code": None,
                        }
                    ],
                    "batch_details": {},
                    "value_timestamp": "2020-01-10T09:00:00Z",
                    "status": "POSTING_INSTRUCTION_BATCH_STATUS_UNKNOWN",
                    "error": None,
                    "insertion_timestamp": "2020-01-10T09:00:00Z",
                    "dry_run": False,
                }
            ],
            "balances": {
                "1": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "1",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-10T09:00:00Z",
                            "amount": "-50",
                            "total_debit": "50",
                            "total_credit": "0",
                        }
                    ]
                },
                "Main account": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-10T09:00:00Z",
                            "amount": "50",
                            "total_debit": "0",
                            "total_credit": "50",
                        }
                    ]
                },
            },
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-11T01:02:03Z",
            "logs": [
                'processed scheduled event "ACCRUE_INTEREST_AND_DAILY_FEES" '
                'for account "Main account"'
            ],
            "posting_instruction_batches": [],
            "balances": {},
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
    {
        "result": {
            "timestamp": "2020-01-11T01:02:03Z",
            "logs": [],
            "posting_instruction_batches": [
                {
                    "id": "3d2e77e8-cf6d-4aa6-8991-3f545e45c671",
                    "create_request_id": "",
                    "client_id": "CoreContracts",
                    "client_batch_id": "ACCRUE_INTEREST_AND_DAILY_FEES_Main "
                    "account_5_ACCRUE_INTEREST_AND_DAILY_FEES_1578704523000000000",
                    "posting_instructions": [
                        {
                            "id": "3a779526-edb9-41a8-a420-f61276e9757b",
                            "client_transaction_id": "ACCRUE_DEPOSIT_INTEREST_TIER1_Main account"
                            "_5_ACCRUE_INTEREST_AND_DAILY_FEES_1578704523000000000_GBP_INTERNAL",
                            "custom_instruction": {
                                "postings": [
                                    {
                                        "credit": False,
                                        "amount": "0.00685",
                                        "denomination": "GBP",
                                        "account_id": "1",
                                        "account_address": "DEFAULT",
                                        "asset": "COMMERCIAL_BANK_MONEY",
                                        "phase": "POSTING_PHASE_COMMITTED",
                                    },
                                    {
                                        "credit": True,
                                        "amount": "0.00685",
                                        "denomination": "GBP",
                                        "account_id": "1",
                                        "account_address": "DEFAULT",
                                        "asset": "COMMERCIAL_BANK_MONEY",
                                        "phase": "POSTING_PHASE_COMMITTED",
                                    },
                                ]
                            },
                            "pics": [],
                            "instruction_details": {
                                "description": "Daily deposit interest accrued at 0.01370% on "
                                "balance of 50.00",
                                "event": "ACCRUE_INTEREST_AND_DAILY_FEES",
                                "originating_account_id": "Main account",
                            },
                            "committed_postings": [
                                {
                                    "credit": False,
                                    "amount": "0.00685",
                                    "denomination": "GBP",
                                    "account_id": "1",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                                {
                                    "credit": True,
                                    "amount": "0.00685",
                                    "denomination": "GBP",
                                    "account_id": "1",
                                    "account_address": "DEFAULT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                            ],
                            "posting_violations": [],
                            "account_violations": [],
                            "restriction_violations": [],
                            "contract_violations": [],
                            "override": {
                                "restrictions": {"all": True, "restriction_set_ids": []}
                            },
                            "transaction_code": None,
                        },
                        {
                            "id": "2f606a63-fcec-488d-9aa6-f42f0d700fcd",
                            "client_transaction_id": "ACCRUE_DEPOSIT_INTEREST_TIER1_Main account"
                            "_5_ACCRUE_INTEREST_AND_DAILY_FEES_1578704523000000000_GBP_CUSTOMER",
                            "custom_instruction": {
                                "postings": [
                                    {
                                        "credit": False,
                                        "amount": "0.00685",
                                        "denomination": "GBP",
                                        "account_id": "Main account",
                                        "account_address": "INTERNAL_CONTRA",
                                        "asset": "COMMERCIAL_BANK_MONEY",
                                        "phase": "POSTING_PHASE_COMMITTED",
                                    },
                                    {
                                        "credit": True,
                                        "amount": "0.00685",
                                        "denomination": "GBP",
                                        "account_id": "Main account",
                                        "account_address": "ACCRUED_DEPOSIT",
                                        "asset": "COMMERCIAL_BANK_MONEY",
                                        "phase": "POSTING_PHASE_COMMITTED",
                                    },
                                ]
                            },
                            "pics": [],
                            "instruction_details": {
                                "description": "Daily deposit interest accrued at 0.01370% on "
                                "balance of 50.00",
                                "event": "ACCRUE_INTEREST_AND_DAILY_FEES",
                                "originating_account_id": "Main account",
                            },
                            "committed_postings": [
                                {
                                    "credit": False,
                                    "amount": "0.00685",
                                    "denomination": "GBP",
                                    "account_id": "Main account",
                                    "account_address": "INTERNAL_CONTRA",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                                {
                                    "credit": True,
                                    "amount": "0.00685",
                                    "denomination": "GBP",
                                    "account_id": "Main account",
                                    "account_address": "ACCRUED_DEPOSIT",
                                    "asset": "COMMERCIAL_BANK_MONEY",
                                    "phase": "POSTING_PHASE_COMMITTED",
                                },
                            ],
                            "posting_violations": [],
                            "account_violations": [],
                            "restriction_violations": [],
                            "contract_violations": [],
                            "override": {
                                "restrictions": {"all": True, "restriction_set_ids": []}
                            },
                            "transaction_code": None,
                        },
                    ],
                    "batch_details": {},
                    "value_timestamp": "2020-01-11T01:02:02.999999Z",
                    "status": "POSTING_INSTRUCTION_BATCH_STATUS_UNKNOWN",
                    "error": None,
                    "insertion_timestamp": "2020-01-11T01:02:03Z",
                    "dry_run": False,
                }
            ],
            "balances": {
                "1": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "1",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T01:02:02.999999Z",
                            "amount": "-50",
                            "total_debit": "50.00685",
                            "total_credit": "0.00685",
                        }
                    ]
                },
                "Main account": {
                    "balances": [
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "DEFAULT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-10T09:00:00Z",
                            "amount": "50",
                            "total_debit": "0",
                            "total_credit": "50",
                        },
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "INTERNAL_CONTRA",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T01:02:02.999999Z",
                            "amount": "-0.00685",
                            "total_debit": "0.00685",
                            "total_credit": "0",
                        },
                        {
                            "id": "",
                            "account_id": "Main account",
                            "account_address": "ACCRUED_DEPOSIT",
                            "phase": "POSTING_PHASE_COMMITTED",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "denomination": "GBP",
                            "posting_instruction_batch_id": "",
                            "update_posting_instruction_batch_id": "",
                            "value_time": "2020-01-11T01:02:02.999999Z",
                            "amount": "0.00685",
                            "total_debit": "0",
                            "total_credit": "0.00685",
                        },
                    ]
                },
            },
            "account_notes": {},
            "instantiate_workflow_requests": {},
            "derived_params": {},
        }
    },
]
