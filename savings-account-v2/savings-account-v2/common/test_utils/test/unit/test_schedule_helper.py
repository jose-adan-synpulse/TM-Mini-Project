import common.test_utils.endtoend as endtoend
from unittest.mock import patch
from unittest import TestCase
from datetime import datetime, timezone

# Declare Constants
REPONSE_EXPECTED_EMPTY = []

RESPONSE_EXPECTED_ALL = [
    {
        "id": "9b33035de688e76ddf9ef2c24f442e62",
        "status": "JOB_STATUS_SUCCEEDED",
        "schedule_id": "43344845-b7e1-413b-9232-96e9aa7165c0",
        "schedule_timestamp": "2021-03-03T23:59:59Z",
        "publish_timestamp": "2021-03-07T00:00:08Z",
    },
    {
        "id": "9608c9340e84ac4ec348820132f909e2",
        "status": "JOB_STATUS_FAILED",
        "schedule_id": "43344845-b7e1-413b-9232-96e9aa7165c0",
        "schedule_timestamp": "2021-03-02T23:59:59Z",
        "publish_timestamp": "2021-03-06T00:00:10Z",
    },
    {
        "id": "890a35253d1beec0bb28e1f722aaf80f",
        "status": "JOB_STATUS_SUCCEEDED",
        "schedule_id": "43344845-b7e1-413b-9232-96e9aa7165c0",
        "schedule_timestamp": "2021-03-03T23:59:59Z",
        "publish_timestamp": "2021-03-05T00:00:13Z",
    },
]

RESPONSE_UNEXPECTED_ALL = [
    {
        "id": "9b33035de688e76ddf9ef2c24f442e62",
        "status": "JOB_STATUS_PUBLISHED",
        "schedule_id": "43344845-b7e1-413b-9232-96e9aa7165c0",
        "schedule_timestamp": "2021-03-03T23:59:59Z",
        "publish_timestamp": "2021-03-01T01:01:17Z",
    },
    {
        "id": "9608c9340e84ac4ec348820132f909e2",
        "status": "JOB_STATUS_PUBLISHED",
        "schedule_id": "43344845-b7e1-413b-9232-96e9aa7165c0",
        "schedule_timestamp": "2021-03-02T23:59:59Z",
        "publish_timestamp": "2021-03-08T00:00:10Z",
    },
    {
        "id": "890a35253d1beec0bb28e1f722aaf80f",
        "status": "JOB_STATUS_PUBLISHED",
        "schedule_id": "43344845-b7e1-413b-9232-96e9aa7165c0",
        "schedule_timestamp": "2021-03-01T01:01:17Z",
        "publish_timestamp": "2021-03-05T00:00:13Z",
    },
]
RESPONSE_MIX = [
    {
        "id": "9b33035de688e76ddf9ef2c24f442e62",
        "status": "JOB_STATUS_SUCCEEDED",
        "schedule_id": "43344845-b7e1-413b-9232-96e9aa7165c0",
        "schedule_timestamp": "2021-03-04T23:59:59Z",
        "publish_timestamp": "2021-03-09T00:00:08Z",
    },
    {
        "id": "9608c9340e84ac4ec348820132f909e2",
        "status": "JOB_STATUS_FAILED",
        "schedule_id": "43344845-b7e1-413b-9232-96e9aa7165c0",
        "schedule_timestamp": "2021-03-04T23:59:59Z",
        "publish_timestamp": "2021-03-09T00:00:10Z",
    },
    {
        "id": "890a35253d1beec0bb28e1f722aaf80f",
        "status": "JOB_STATUS_PUBLISHED",
        "schedule_id": "43344845-b7e1-413b-9232-96e9aa7165c0",
        "schedule_timestamp": "2021-03-01T01:01:17Z",
        "publish_timestamp": "2021-03-01T00: 00:13Z",
    },
]


class ScheduleHelperTest(TestCase):
    def setUp(self) -> None:

        return super().setUp()

    @patch.object(endtoend.core_api_helper, "get_jobs")
    def test_wait_scheduled_jobs_passed_all_dates_empty(
        self,
        get_jobs,
    ):
        # This will test for empty response, should fail since there are no valid pending jobs
        # no effective date so all will be considered
        get_jobs.side_effect = [REPONSE_EXPECTED_EMPTY]
        result = endtoend.schedule_helper._check_jobs_status(
            "8c620041-00a1-4a0e-8940-1714bc1fdfc2"
        )
        self.assertEqual(result, False)

    @patch.object(endtoend.core_api_helper, "get_jobs")
    def test_wait_scheduled_jobs_passed_all_dates(self, get_jobs):
        # This will test that it will return true if all jobs are finished.
        # It should return True no effective date so all will be considered.

        get_jobs.side_effect = [RESPONSE_EXPECTED_ALL]
        result = endtoend.schedule_helper._check_jobs_status(
            "8c620041-00a1-4a0e-8940-1714bc1fdfc2"
        )
        # Should succeed
        self.assertEqual(result, True)

    @patch.object(endtoend.core_api_helper, "get_jobs")
    def test_wait_scheduled_jobs_specific_day(self, get_jobs):
        # This will test for effective date scenario, where the effective date
        # given is down to the day. Since the time stamp returned by vault
        # is down to the second there will be no date matching this day.
        # There are no valid jobs on that date so it should fail.
        get_jobs.side_effect = [RESPONSE_UNEXPECTED_ALL]
        result = endtoend.schedule_helper._check_jobs_status(
            "8c620041-00a1-4a0e-8940-1714bc1fdfc2",
            datetime(year=2021, month=3, day=1, tzinfo=timezone.utc),
        )
        self.assertEqual(result, False)

    @patch.object(endtoend.core_api_helper, "get_jobs")
    def test_wait_scheduled_jobs_specific_second(self, get_jobs):
        # This will test for effective date scenario where the effective date given is down to the
        # second where in there are no valid jobs on that date so it should fail
        get_jobs.side_effect = [RESPONSE_UNEXPECTED_ALL]
        result = endtoend.schedule_helper._check_jobs_status(
            "8c620041-00a1-4a0e-8940-1714bc1fdfc2",
            datetime(
                year=2021,
                month=3,
                day=1,
                hour=1,
                minute=1,
                second=17,
                tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(result, False)

    @patch.object(endtoend.core_api_helper, "get_jobs")
    def test_wait_scheduled_jobs_pending_all_date(self, get_jobs):
        # This will test that scenario where there are still pending jobs for all dates
        get_jobs.side_effect = [RESPONSE_UNEXPECTED_ALL]

        result = endtoend.schedule_helper._check_jobs_status(
            "8c620041-00a1-4a0e-8940-1714bc1fdfc2"
        )
        self.assertEqual(result, False)

    @patch.object(endtoend.core_api_helper, "get_jobs")
    def test_wait_scheduled_jobs_mix_all_date(self, get_jobs):
        # This will test that scenario where there is a mix of pending and finish jobs for all dates
        get_jobs.side_effect = [RESPONSE_MIX]

        result = endtoend.schedule_helper._check_jobs_status(
            "8c620041-00a1-4a0e-8940-1714bc1fdfc2"
        )
        self.assertEqual(result, False)

    @patch.object(endtoend.core_api_helper, "get_jobs")
    def test_wait_scheduled_jobs_pending_specific_date(self, get_jobs):
        # This will test for effective date scenario where in there is still a pending job
        get_jobs.side_effect = [RESPONSE_UNEXPECTED_ALL]

        result = endtoend.schedule_helper._check_jobs_status(
            "8c620041-00a1-4a0e-8940-1714bc1fdfc2",
            datetime(
                year=2021,
                month=3,
                day=1,
                hour=1,
                minute=1,
                second=17,
                tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(result, False)

    @patch.object(endtoend.core_api_helper, "get_jobs")
    def test_wait_scheduled_jobs_passed_specific_date(self, get_jobs):
        # This will test for effective date scenario where in there are no pending job
        get_jobs.side_effect = [RESPONSE_EXPECTED_ALL]

        result = endtoend.schedule_helper._check_jobs_status(
            "8c620041-00a1-4a0e-8940-1714bc1fdfc2",
            datetime(
                year=2021,
                month=3,
                day=2,
                hour=23,
                minute=59,
                second=59,
                tzinfo=timezone.utc,
            ),
        )
        self.assertEqual(result, True)
