# standard libs
from dataclasses import dataclass, field
from typing import List
from unittest import TestCase
from unittest.mock import Mock, patch

# common
import common.kafka.kafka as kafka
from common.test_utils.common.utils import create_mock_message_queue

SAMPLE_BALANCE_MESSAGE = (
    "common/kafka/tests/unit/input/sample_balance_update_event.json"
)
SAMPLE_WF_MESSAGE = "common/kafka/tests/unit/input/sample_wf_instantiation_event.json"


@dataclass
class DummyClassForTest:
    workflow_definition_id: str
    count: int
    request_ids: List[str] = field(default_factory=list)


class KafkaTest(TestCase):
    def setUp(self) -> None:
        return super().setUp()

    def test_wait_for_messages(self):
        def matcher(event_msg, unique_message_ids):
            account_id = event_msg["account_id"]
            request_id = event_msg["event_id"]
            return (
                account_id,
                request_id,
                True if account_id in unique_message_ids else ("", request_id, False),
            )

        message_ids = {"1": None, "2": None, "3": None}

        consumer = create_mock_message_queue(SAMPLE_BALANCE_MESSAGE)

        result = kafka.wait_for_messages(
            consumer=consumer,
            matcher=matcher,
            callback=None,
            unique_message_ids=message_ids,
            matched_message_timeout=3,
            inter_message_timeout=3,
        )
        expected_result = {"2": None, "3": None}
        self.assertEqual(result, expected_result)

    def test_wait_for_messages_with_dict_(self):
        def matcher(event_msg, unique_message_ids):
            event_request_id = event_msg["request_id"]
            event_wf_definition_id = event_msg["workflow_instance"][
                "workflow_definition_id"
            ]
            event_account_id = event_msg["instantiation_context"].get("account_id")
            is_matched = False
            if event_account_id in unique_message_ids:
                for expected_workflow in unique_message_ids[event_account_id]:
                    if (
                        expected_workflow.workflow_definition_id
                        == event_wf_definition_id
                        and event_request_id not in expected_workflow.request_ids
                    ):
                        expected_workflow.count -= 1
                        expected_workflow.request_ids.append(event_request_id)
                        is_matched = True
                        break

            return None, event_request_id, is_matched

        message_ids = {
            "1": [
                DummyClassForTest(workflow_definition_id="TEST_WF", count=1),
                DummyClassForTest(workflow_definition_id="TEST_WF_2", count=2),
            ],
            "2": [
                DummyClassForTest(workflow_definition_id="TEST_WF", count=1),
                DummyClassForTest(workflow_definition_id="TEST_WF_2", count=2),
            ],
        }
        consumer = create_mock_message_queue(SAMPLE_WF_MESSAGE)

        result = kafka.wait_for_messages(
            consumer=consumer,
            matcher=matcher,
            callback=None,
            unique_message_ids=message_ids,
            matched_message_timeout=1,
            inter_message_timeout=1,
        )
        expected_result = {
            "1": [
                DummyClassForTest(
                    workflow_definition_id="TEST_WF",
                    count=0,
                    request_ids=["123test123"],
                ),
                DummyClassForTest(
                    workflow_definition_id="TEST_WF_2", count=2, request_ids=[]
                ),
            ],
            "2": [
                DummyClassForTest(
                    workflow_definition_id="TEST_WF", count=1, request_ids=[]
                ),
                DummyClassForTest(
                    workflow_definition_id="TEST_WF_2", count=2, request_ids=[]
                ),
            ],
        }
        self.assertDictEqual(result, expected_result)

    @patch("logging.Logger.warning")
    def test_wait_for_messages_matched_message_timeout(self, warning_logging: Mock):
        def matcher(event_msg, unique_message_ids):
            account_id = event_msg["account_id"]
            request_id = event_msg["event_id"]
            return (
                account_id,
                request_id,
                True if account_id in unique_message_ids else ("", request_id, False),
            )

        message_ids = {"1": None, "2": None, "3": None}

        consumer = create_mock_message_queue(
            SAMPLE_BALANCE_MESSAGE, matched_message_sleep=2, while_loop_sleep=0
        )

        kafka.wait_for_messages(
            consumer=consumer,
            matcher=matcher,
            callback=None,
            unique_message_ids=message_ids,
            matched_message_timeout=1,
            inter_message_timeout=0,
        )
        warning_logging.assert_called_with(
            f"Waited 2.0s since last matched message received. "
            f"Timeout set to 1.0. Exiting after 1 "
            f"messages received"
        )

    @patch("logging.Logger.warning")
    def test_wait_for_messages_inter_message_timeout(self, warning_logging: Mock):
        def matcher(event_msg, unique_message_ids):
            account_id = event_msg["account_id"]
            request_id = event_msg["event_id"]
            return (
                account_id,
                request_id,
                True if account_id in unique_message_ids else ("", request_id, False),
            )

        message_ids = {"1": None, "2": None, "3": None}

        consumer = create_mock_message_queue(SAMPLE_BALANCE_MESSAGE)

        kafka.wait_for_messages(
            consumer=consumer,
            matcher=matcher,
            callback=None,
            unique_message_ids=message_ids,
            matched_message_timeout=0,
            inter_message_timeout=1,
        )

        warning_logging.assert_called_with(
            f"Waited 2.0s since last message received. "
            f"Timeout set to 1.0. Exiting after 1 "
            f"messages received"
        )
