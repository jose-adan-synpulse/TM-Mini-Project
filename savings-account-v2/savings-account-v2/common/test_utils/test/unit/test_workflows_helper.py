# standard libs
from unittest import TestCase
from unittest.mock import patch
from copy import deepcopy

# third party
import yaml

# common
from common.test_utils.common.utils import create_mock_message_queue
import common.test_utils.endtoend.workflows_helper as workflows_helper
from common.test_utils.performance.data_objects.data_objects import (
    ExpectedWorklowInstantiation,
)

# This definition is not really valid, but contains the states we need to test
WORKFLOW_DEFINITION = """
---
name: Name
instance_title: Title
description: Description
schema_version: 2.3.0
definition_version: 2.3.4
starting_state: check_if_new_customer
end_states:
  - state: account_opened_successfully
    result: SUCCESSFUL
  - state: account_application_rejected
    result: FAILED

states:
  child_workflow_state:
    entry_actions:
      instantiate_workflow:
        definition_id: APPLY_FOR_EASY_ACCESS_SAVER
        definition_version: 1.8.0

  plan_state:
    state_name: create_plan
    entry_actions:
      vault_callback:
        arguments:
          plan:
            supervisor_contract_version_id: '&{offset_mortgage_supervisor_contract_version}'

  calendar_state:
    entry_actions:
      vault_callback:
        arguments:
          calendar_ids:
            - BACS

  product_state:
    entry_actions:
      vault_callback:
        arguments:
          account:
            product_id: us_checking_account

  internal_account_state:
    entry_actions:
      vault_callback:
        arguments:
          posting_instruction_batch:
            posting_instructions:
              - outbound_hard_settlement:
                  internal_account_id: "1"
"""


SAMPLE_WF_MESSAGE = (
    "common/test_utils/test/unit/input/sample_wf_instantiation_event.json"
)


class WorkflowHelperTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.base_definition = yaml.safe_load(WORKFLOW_DEFINITION)

    def setUp(self) -> None:
        self.definition = deepcopy(self.base_definition)

    def test_replace_calendar_ids(
        self,
    ):
        new_definition = workflows_helper.replace_calendar_ids(
            self.definition,
            calendar_id_to_uploaded_id={"BACS": "E2E_BACS_ID"},
        )
        create_calendar = new_definition["states"]["calendar_state"]["entry_actions"][
            "vault_callback"
        ]["arguments"]
        self.assertEqual(create_calendar["calendar_ids"], ["E2E_BACS_ID"])

    def test_replace_child_workflow_definition_ids(
        self,
    ):
        new_definition = workflows_helper.replace_child_workflow_definition_ids(
            self.definition,
            workflow_definition_id_mapping={"APPLY_FOR_EASY_ACCESS_SAVER": "E2E_WF_ID"},
        )
        workflow_instantiation = new_definition["states"]["child_workflow_state"][
            "entry_actions"
        ]["instantiate_workflow"]
        self.assertEqual(workflow_instantiation["definition_id"], "E2E_WF_ID")
        self.assertEqual(workflow_instantiation["definition_version"], "1.0.0")

    def test_replace_internal_account_ids(
        self,
    ):
        new_definition = workflows_helper.replace_internal_account_ids(
            self.definition,
            internal_account_id_to_uploaded_id={"1": "E2E_1"},
        )
        posting_instruction_batch = new_definition["states"]["internal_account_state"][
            "entry_actions"
        ]["vault_callback"]["arguments"]["posting_instruction_batch"]
        self.assertEqual(
            posting_instruction_batch["posting_instructions"][0][
                "outbound_hard_settlement"
            ]["internal_account_id"],
            "E2E_1",
        )

    def test_replace_product_ids(
        self,
    ):
        new_definition = workflows_helper.replace_product_ids(
            self.definition,
            contract_pid_to_uploaded_pid={
                "us_checking_account": "E2E_us_checking_account"
            },
        )
        product_id = new_definition["states"]["product_state"]["entry_actions"][
            "vault_callback"
        ]["arguments"]["account"]["product_id"]
        self.assertEqual(
            product_id,
            "E2E_us_checking_account",
        )

    def test_replace_supervisor_contract_version_ids(
        self,
    ):
        new_definition = workflows_helper.replace_supervisor_contract_version_ids(
            self.definition,
            supervisorcontract_name_to_id={"offset_mortgage": "BwrqXkvhfS"},
        )
        product_id = new_definition["states"]["plan_state"]["entry_actions"][
            "vault_callback"
        ]["arguments"]["plan"]["supervisor_contract_version_id"]
        self.assertEqual(
            product_id,
            "BwrqXkvhfS",
        )

    def test_generate_expected_workflows_object_dict(self):
        workflows = {"id_1": 1, "id_2": 2, "id_3": 3}
        account_ids = ["1", "2", "3", "1"]

        result = workflows_helper._generate_expected_workflows_object_dict(
            account_ids=account_ids, expected_wf=workflows
        )

        expected_result = {
            "1": [
                ExpectedWorklowInstantiation(workflow_definition_id="id_1", count=1),
                ExpectedWorklowInstantiation(workflow_definition_id="id_2", count=2),
                ExpectedWorklowInstantiation(workflow_definition_id="id_3", count=3),
            ],
            "2": [
                ExpectedWorklowInstantiation(workflow_definition_id="id_1", count=1),
                ExpectedWorklowInstantiation(workflow_definition_id="id_2", count=2),
                ExpectedWorklowInstantiation(workflow_definition_id="id_3", count=3),
            ],
            "3": [
                ExpectedWorklowInstantiation(workflow_definition_id="id_1", count=1),
                ExpectedWorklowInstantiation(workflow_definition_id="id_2", count=2),
                ExpectedWorklowInstantiation(workflow_definition_id="id_3", count=3),
            ],
        }

        self.assertDictEqual(result, expected_result)

    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.api.v1.workflows.workflow_instance.create.requests": create_mock_message_queue(
                SAMPLE_WF_MESSAGE
            ),
        },
    )
    def test_wait_for_wf_instantiation_messages_missing_wfs(self):

        expected_workflows = {"id_2": 2, "id_3": 3}

        account_ids = ["1", "2", "3"]

        result = workflows_helper.wait_for_wf_instantiation_messages(
            account_ids, expected_workflows, 1, 1
        )

        expected_result = {
            "1": {
                "id_1": {"expected": 0, "seen": 1},
                "id_2": {"expected": 2, "seen": 0},
                "id_3": {"expected": 3, "seen": 0},
            },
            "2": {
                "id_2": {"expected": 2, "seen": 0},
                "id_3": {"expected": 3, "seen": 0},
            },
            "3": {
                "id_2": {"expected": 2, "seen": 0},
                "id_3": {"expected": 3, "seen": 0},
            },
        }

        self.assertTrue(result == expected_result)

    @patch.dict(
        "common.test_utils.endtoend.testhandle.kafka_consumers",
        {
            "vault.api.v1.workflows.workflow_instance.create.requests": create_mock_message_queue(
                SAMPLE_WF_MESSAGE
            ),
        },
    )
    def test_wait_for_wf_instantiation_messages_no_missing_wfs(self):

        expected_workflows = {"id_1": 1}

        account_ids = ["1"]

        result = workflows_helper.wait_for_wf_instantiation_messages(
            account_ids, expected_workflows, 1, 1
        )
        self.assertTrue(result == {})
