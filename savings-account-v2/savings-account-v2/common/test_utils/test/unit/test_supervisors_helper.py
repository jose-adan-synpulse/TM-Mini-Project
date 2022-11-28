# standard libs
import json
from unittest import TestCase
from unittest.mock import patch, Mock

# common
import common.test_utils.endtoend.supervisors_helper as supervisors_helper

NORMAL_ASSOCIATIONS = "common/test_utils/test/unit/input/normal_plan_associations.json"
MULTI_ASSOCIATIONS = "common/test_utils/test/unit/input/multi_plan_associations.json"


class SupervisorsHelperTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open(NORMAL_ASSOCIATIONS, "r", encoding="utf-8") as assoc_file:
            cls.normal_plan_associations = json.load(assoc_file)
        with open(MULTI_ASSOCIATIONS, "r", encoding="utf-8") as assoc_file:
            cls.multi_plan_associations = json.load(assoc_file)

    @patch.object(supervisors_helper, "get_plan_associations")
    def test_check_plan_associations_passes_with_list_input(
        self, get_plan_associations: Mock
    ):

        get_plan_associations.return_value = self.normal_plan_associations

        expected_associations = [
            "a93de940-6300-a146-a71d-9b5389ee89a3",
            "1e3f7dfe-8f92-c2db-c62d-d58a6e6991f3",
            "22f72eed-9a16-7526-5fc3-8ff0278b8d62",
        ]
        try:
            supervisors_helper.check_plan_associations(
                self,
                plan_id="6f957686-05e8-2488-174a-63a11a460372",
                accounts=expected_associations,
            )
        except AssertionError as error:
            self.fail(f"Unexpected AssertionError raised: {error}")

    @patch.object(supervisors_helper, "get_plan_associations")
    def test_check_plan_associations_passes_with_dict_input(
        self, get_plan_associations: Mock
    ):

        get_plan_associations.return_value = self.normal_plan_associations

        expected_associations = {
            "a93de940-6300-a146-a71d-9b5389ee89a3": "ACCOUNT_PLAN_ASSOC_STATUS_ACTIVE",
            "1e3f7dfe-8f92-c2db-c62d-d58a6e6991f3": "ACCOUNT_PLAN_ASSOC_STATUS_ACTIVE",
            "22f72eed-9a16-7526-5fc3-8ff0278b8d62": "ACCOUNT_PLAN_ASSOC_STATUS_ACTIVE",
        }

        try:
            supervisors_helper.check_plan_associations(
                self,
                plan_id="6f957686-05e8-2488-174a-63a11a460372",
                accounts=expected_associations,
            )
        except AssertionError as error:
            self.fail(f"Unexpected AssertionError raised: {error}")

    @patch.object(supervisors_helper, "get_plan_associations")
    def test_check_plan_associations_fails_with_incorrect_status(
        self, get_plan_associations: Mock
    ):

        get_plan_associations.return_value = self.normal_plan_associations

        expected_associations = {
            "a93de940-6300-a146-a71d-9b5389ee89a3": "ACCOUNT_PLAN_ASSOC_STATUS_ACTIVE",
            "1e3f7dfe-8f92-c2db-c62d-d58a6e6991f3": "ACCOUNT_PLAN_ASSOC_STATUS_INACTIVE",
            "22f72eed-9a16-7526-5fc3-8ff0278b8d62": "ACCOUNT_PLAN_ASSOC_STATUS_ACTIVE",
        }
        with self.assertRaises(AssertionError):
            supervisors_helper.check_plan_associations(
                self,
                plan_id="6f957686-05e8-2488-174a-63a11a460372",
                accounts=expected_associations,
            )

    @patch.object(supervisors_helper, "get_plan_associations")
    def test_check_plan_associations_fails_with_incorrect_account_id(
        self, get_plan_associations: Mock
    ):

        get_plan_associations.return_value = self.normal_plan_associations

        expected_associations = [
            "a93de940-6300-a146-a71d-9b5389ee89a3",
            "1e3f7dfe-8f92-c2db-c62d-d58a6e6991f3",
            "22f72eed-9a16-7526",
        ]
        with self.assertRaises(AssertionError):
            supervisors_helper.check_plan_associations(
                self,
                plan_id="6f957686-05e8-2488-174a-63a11a460372",
                accounts=expected_associations,
            )

    @patch.object(supervisors_helper, "get_plan_associations")
    def test_check_plan_associations_uses_latest_association(
        self, get_plan_associations: Mock
    ):

        get_plan_associations.return_value = self.multi_plan_associations

        # the multiple associations has an active and inactive entry for 1e3f7...
        # as the inactive one comes last it's the only one that's preserved
        expected_associations = {
            "a93de940-6300-a146-a71d-9b5389ee89a3": "ACCOUNT_PLAN_ASSOC_STATUS_ACTIVE",
            "1e3f7dfe-8f92-c2db-c62d-d58a6e6991f3": "ACCOUNT_PLAN_ASSOC_STATUS_INACTIVE",
            "22f72eed-9a16-7526-5fc3-8ff0278b8d62": "ACCOUNT_PLAN_ASSOC_STATUS_ACTIVE",
        }

        try:
            supervisors_helper.check_plan_associations(
                self,
                plan_id="6f957686-05e8-2488-174a-63a11a460372",
                accounts=expected_associations,
            )
        except AssertionError as error:
            self.fail(f"Unexpected AssertionError raised: {error}")
