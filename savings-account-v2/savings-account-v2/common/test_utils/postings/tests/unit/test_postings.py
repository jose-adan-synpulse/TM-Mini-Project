# standard libs
import time
from unittest import TestCase
from unittest.mock import patch

# common
import common.test_utils.endtoend.postings as postings
from common.test_utils.postings.posting_classes import (
    InboundAuthorisation,
    InboundHardSettlement,
    OutboundAuthorisation,
    OutboundHardSettlement,
)


class PostingsTest(TestCase):
    def setUp(self):
        self._started_at = time.time()

    def tearDown(self):
        self._elapsed_time = time.time() - self._started_at

    @patch.dict(
        "common.test_utils.endtoend.testhandle.internal_account_id_to_uploaded_id",
        {
            "DUMMY_CONTRA": "e2e_L_DUMMY_CONTRA",
            "Some internal account": "e2e_L_some_internal_account",
        },
    )
    def test_generate_instruction_inbound(self):
        # This will test to check that the posting methods are using the correct
        # type of posting (inbound/outbound & hard settlement/authorisation)
        # along with checking that the internal account being used is correct.
        # When no internal account is provided, DUMMY_CONTRA should be used as the default
        test_cases = [
            {
                "description": "Default internal account, inbound hard settlement",
                "internal_account": None,
                "is_inbound": True,
                "is_auth": False,
                "expected_results": {
                    "internal_account": "e2e_L_DUMMY_CONTRA",
                    "type": InboundHardSettlement,
                },
            },
            {
                "description": "Provided internal account, inbound hard settlement",
                "internal_account": "Some internal account",
                "is_inbound": True,
                "is_auth": False,
                "expected_results": {
                    "internal_account": "e2e_L_some_internal_account",
                    "type": InboundHardSettlement,
                },
            },
            {
                "description": "Default internal account, outbound hard settlement",
                "internal_account": None,
                "is_inbound": False,
                "is_auth": False,
                "expected_results": {
                    "internal_account": "e2e_L_DUMMY_CONTRA",
                    "type": OutboundHardSettlement,
                },
            },
            {
                "description": "Provided internal account, outbound hard settlement",
                "internal_account": "Some internal account",
                "is_inbound": False,
                "is_auth": False,
                "expected_results": {
                    "internal_account": "e2e_L_some_internal_account",
                    "type": OutboundHardSettlement,
                },
            },
            {
                "description": "Provided internal account, outbound authorisation",
                "internal_account": None,
                "is_inbound": False,
                "is_auth": True,
                "expected_results": {
                    "internal_account": "e2e_L_DUMMY_CONTRA",
                    "type": OutboundAuthorisation,
                },
            },
            {
                "description": "Provided internal account, inbound authorisation",
                "internal_account": "Some internal account",
                "is_inbound": False,
                "is_auth": True,
                "expected_results": {
                    "internal_account": "e2e_L_some_internal_account",
                    "type": OutboundAuthorisation,
                },
            },
            {
                "description": "Provided internal account, inbound authorisation",
                "internal_account": None,
                "is_inbound": True,
                "is_auth": True,
                "expected_results": {
                    "internal_account": "e2e_L_DUMMY_CONTRA",
                    "type": InboundAuthorisation,
                },
            },
            {
                "description": "Provided internal account, inbound authorisation",
                "internal_account": "Some internal account",
                "is_inbound": True,
                "is_auth": True,
                "expected_results": {
                    "internal_account": "e2e_L_some_internal_account",
                    "type": InboundAuthorisation,
                },
            },
        ]

        for test_case in test_cases:
            result = postings.generate_instruction(
                is_inbound=test_case["is_inbound"],
                is_auth=test_case["is_auth"],
                amount=100,
                target_account_id="123",
                internal_account_id=test_case["internal_account"],
            )

            self.assertIsInstance(
                result,
                test_case["expected_results"]["type"],
                test_case["description"],
            )
            self.assertEqual(
                result.internal_account_id,
                test_case["expected_results"]["internal_account"],
                test_case["description"],
            )
