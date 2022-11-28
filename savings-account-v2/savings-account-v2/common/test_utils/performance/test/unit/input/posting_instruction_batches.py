CURRENT_ACCOUNT_ACCRUE_PIBS_1 = [
    {
        "id": "6b5c6422-fa26-4957-92ae-588dcfd00047",
        "create_request_id": "",
        "client_id": "CoreContracts",
        "client_batch_id": "ACCRUE_INTEREST_AND_DAILY_FEES_Main "
        "account_5_ACCRUE_INTEREST_AND_DAILY_FEES_1578704523000000000",
        "posting_instructions": [
            {
                "id": "644ef80c-ca98-4c15-9e3a-b87199b96c38",
                "client_transaction_id": "ACCRUE_DEPOSIT_INTEREST_TIER1_Main "
                "account_5_ACCRUE_INTEREST_AND_DAILY_FEES_1578704523000000000_GBP_CUSTOMER",
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
                    "description": "Daily deposit interest accrued at 0.01370% on balance of 50.00",
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
                "override": {"restrictions": {"all": True, "restriction_set_ids": []}},
                "transaction_code": None,
            },
            {
                "id": "bb9cddc2-e296-4eed-9675-f54b3525dd36",
                "client_transaction_id": "ACCRUE_DEPOSIT_INTEREST_TIER1_Main "
                "account_5_ACCRUE_INTEREST_AND_DAILY_FEES_1578704523000000000_GBP_INTERNAL",
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
                    "description": "Daily deposit interest accrued at 0.01370% on balance of 50.00",
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
                "override": {"restrictions": {"all": True, "restriction_set_ids": []}},
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
]
LOAN_PIBS_1 = [
    {
        "id": "9a25ff64-e05f-4364-9125-a5ff7980283b",
        "create_request_id": "",
        "client_id": "CoreContracts",
        "client_batch_id": "BATCH_Main account_3__1590829200000000000_INITIAL_LOAN_DISBURSMENT",
        "posting_instructions": [
            {
                "id": "e304d113-071f-4a95-88c8-841e08cfa6fc",
                "client_transaction_id": "Main account_3__1590829200000000000_"
                "PRINCIPAL_DISBURSMENT",
                "custom_instruction": {
                    "postings": [
                        {
                            "credit": False,
                            "amount": "1000",
                            "denomination": "GBP",
                            "account_id": "Main account",
                            "account_address": "PRINCIPAL",
                            "asset": "COMMERCIAL_BANK_MONEY",
                            "phase": "POSTING_PHASE_COMMITTED",
                        },
                        {
                            "credit": True,
                            "amount": "1000",
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
                    "description": "Payment of 1000 of loan principal",
                    "event": "PRINCIPAL_PAYMENT",
                    "originating_account_id": "Main account",
                },
                "committed_postings": [
                    {
                        "credit": False,
                        "amount": "1000",
                        "denomination": "GBP",
                        "account_id": "Main account",
                        "account_address": "PRINCIPAL",
                        "asset": "COMMERCIAL_BANK_MONEY",
                        "phase": "POSTING_PHASE_COMMITTED",
                    },
                    {
                        "credit": True,
                        "amount": "1000",
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
                "override": None,
                "transaction_code": None,
            }
        ],
        "batch_details": {},
        "value_timestamp": "2020-05-30T00:00:00Z",
        "status": "POSTING_INSTRUCTION_BATCH_STATUS_UNKNOWN",
        "error": None,
        "insertion_timestamp": "2020-05-30T00:00:00Z",
        "dry_run": False,
    }
]
