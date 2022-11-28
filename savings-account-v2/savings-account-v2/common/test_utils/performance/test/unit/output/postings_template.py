POSTINGS_TEMPLATE = [
    {
        "client_id": "Migration",
        "client_batch_id": "123",
        "posting_instructions": [
            {
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
                "override": {"restrictions": None},
                "transaction_code": None,
            }
        ],
        "batch_details": {},
        "value_timestamp": "2020-01-10T09:00:00Z",
        "dry_run": False,
    },
    {
        "client_id": "Migration",
        "client_batch_id": "123",
        "posting_instructions": [
            {
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
                "override": {"restrictions": None},
                "transaction_code": None,
            }
        ],
        "batch_details": {},
        "value_timestamp": "2020-01-11T09:00:00Z",
        "dry_run": False,
    },
]
