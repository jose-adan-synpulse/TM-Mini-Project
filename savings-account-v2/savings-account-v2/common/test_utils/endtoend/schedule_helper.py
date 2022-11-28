# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
# standard libs
import os
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

# third party
from dateutil.relativedelta import relativedelta
from dateutil import parser

# common
import common.test_utils.endtoend as endtoend
from common.test_utils.endtoend.kafka_helper import wait_for_messages

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


SCHEDULER_OPERATION_EVENTS_TOPIC = "vault.core_api.v1.scheduler.operation.events"


def wait_for_schedule_operation_events(
    tag_names: List[str],
    inter_message_timeout: int = 30,
    matched_message_timeout: int = 30,
):

    consumer = endtoend.testhandle.kafka_consumers[SCHEDULER_OPERATION_EVENTS_TOPIC]
    mapped_tags = {
        endtoend.testhandle.schedule_tag_ids_to_e2e_ids[tag]: None for tag in tag_names
    }

    def matcher(event_msg, unique_message_ids):
        # we also get account_update_created events on this topic
        tag_name = event_msg["operation_created"]["operation"]["tag_name"]
        event_request_id = event_msg["event_id"]
        if tag_name in unique_message_ids:
            return tag_name, event_request_id, True
        else:
            return "", event_request_id, False

    log.info(f"Waiting for {len(mapped_tags)} operation events: {mapped_tags}")

    wait_for_messages(
        consumer,
        matcher=matcher,
        callback=None,
        unique_message_ids=mapped_tags,
        inter_message_timeout=inter_message_timeout,
        matched_message_timeout=matched_message_timeout,
    )

    log.info("Got all operation events")


def trigger_next_schedule_execution(
    paused_tag_id: str,
    schedule_frequency: str,
) -> None:
    """
    Triggers the next schedule execution by updating a paused tag. The frequency is used to
    determine how much the test_pause_at_timestamp should be incremented by to guarantee a
    desired number of executions
    :param paused_tag_id: unmapped id of the paused tag that controls the schedule. The
    corresponding mapped tag must have a 'test_pause_at_timestamp' value that is preventing the
    execution of a schedule.
    :param schedule_frequency: frequency of the schedule. Supported values are 'DAILY', 'WEEKLY'
    'MONTHLY' and 'YEARLY'
    """
    mapped_tag_id = endtoend.testhandle.schedule_tag_ids_to_e2e_ids[paused_tag_id]

    current_tag = endtoend.core_api_helper.batch_get_account_schedule_tags(
        account_schedule_tag_ids=[mapped_tag_id]
    )[mapped_tag_id]

    test_pause_at_timestamp = current_tag["test_pause_at_timestamp"]

    if schedule_frequency == "DAILY":
        delta = relativedelta(days=1)
    elif schedule_frequency == "WEEKLY":
        delta = relativedelta(weeks=1)
    elif schedule_frequency == "MONTHLY":
        delta = relativedelta(months=1)
    elif schedule_frequency == "YEARLY":
        delta = relativedelta(years=1)
    else:
        raise ValueError(f"Unexpected frequency {schedule_frequency}")

    # Setting test_pause_at_timestamp in the future currently fast-forwards, so we make sure it
    # is at most now()
    new_test_pause_at_timestamp = min(
        delta + parser.parse(test_pause_at_timestamp), datetime.now(tz=timezone.utc)
    )

    log.info(
        f"Updating test_pause_at_timestamp from {test_pause_at_timestamp}"
        f" to {new_test_pause_at_timestamp} for {mapped_tag_id}"
    )

    endtoend.core_api_helper.update_account_schedule_tag(
        account_schedule_tag_id=mapped_tag_id,
        test_pause_at_timestamp=new_test_pause_at_timestamp.isoformat(),
    )


def _check_jobs_status(
    schedule_id: str,
    effective_date: Optional[datetime] = None,
    expected_status: Optional[List[str]] = None,
) -> bool:
    """
    Checks status of all jobs in a schedule.
    :param schedule_id: The ID of the Schedule.
    :param expected_status: All jobs must be of this status.
    :param effective_date: An optional job effective date, helps filter further in case multiple.
    jobs are triggered in the test. if left blank it will check all the dates.
    :return: True if jobs with schedule_timestamp == effective date status is present in
    expected_status else return False, in case no valid jobs were found will also return false.
    """

    expected_status = expected_status or ["JOB_STATUS_SUCCEEDED", "JOB_STATUS_FAILED"]
    result = endtoend.core_api_helper.get_jobs(schedule_id)
    flag_no_jobs = True

    for job in result:
        job_time_stamp = job["schedule_timestamp"]
        job_time_stamp = parser.parse(job_time_stamp)
        if effective_date is None or effective_date == job_time_stamp:
            flag_no_jobs = False
            if job["status"].upper() not in expected_status:
                return False
    if flag_no_jobs:
        return False
    return True


def wait_for_scheduled_jobs_to_finish(
    schedule_display_name: str,
    account_id: str,
    effective_date: Optional[datetime] = None,
    max_retries: int = 7,
    initial_wait: int = 0,
    job_statuses: List[str] = None,
):
    """
    Polls when all the jobs with same effective date in the schedule are finished.
    It will retry this for 5 times by default, each retry will multiply the current sleep
    by the current sleep so for this case the total time for the polling is = 2^7 total
    seconds which is 128 (or just over 2 mins)
    :param schedule_name: The Display Name of the Schedule, as per the contract event type. The
    'for account...' suffix is automatically removed if present
    :param account_id: The account ID of the schedule.
    :param effective_date: An optional parameter, helps filter further in case multiple
    jobs are triggered in the test. if left blank it will check all the dates.
    :param max_retries: The number of times to poll before abandoning
    :param initial_wait: an initial number of seconds to wait for before starting to poll. This is
    useful if we know there is a long delay before the results will ever be met. For example, if
    waiting for 30 jobs to skip, we know there is a 30*20 wait just for those jobs to be published
    :param job_statuses: optional list of job statuses to consider. This can be useful if we want
    to check a job was skipped, but it will default to JOB_STATUS_SUCCEEDED and JOB_STATUS_FAILED
    :return: if jobs are finished [JOB_STATUS_SUCCEEDED', 'JOB_STATUS_FAILED']
    do nothing else after multiple attemps raise an assertion
    """

    job_statuses = job_statuses or ["JOB_STATUS_SUCCEEDED", "JOB_STATUS_FAILED"]
    schedule_name = schedule_display_name.replace(f" for {account_id}", "")
    log.info(
        f"Waiting for {schedule_name} on account {account_id} to reach statuses {job_statuses}"
        f"Optional effective_date: {effective_date}"
    )

    if initial_wait > 0:
        time.sleep(initial_wait)

    schedules = endtoend.core_api_helper.get_account_schedules(account_id, [])
    schedule_id = schedules[schedule_name]["id"]

    error_message = f"Schedule=[{schedule_display_name}] with ID=[{schedule_id}] "
    error_message += f"still has pending jobs for Account ID=[{account_id}]"
    error_message += f' on effective date = {effective_date  or "ALL"}'

    if schedule_id is not None:
        endtoend.helper.retry_call(
            func=_check_jobs_status,
            f_args=[schedule_id, effective_date, job_statuses],
            expected_result=True,
            back_off=2,
            max_retries=max_retries,
            failure_message=error_message,
        )


def set_test_pause_at_timestamp_for_tag(
    schedule_tag_id: str, test_pause_at_timestamp=datetime
):
    """
    Updates the test_pause_at_timestamp for a given schedule tag ID as defined in the Smart
    Contract. Maps the provided schedule tag to the actual E2E schedule tag used in the test
    before sending the request.
    :param schedule_tag_id: The schedule tag ID as defined in the Smart Contract.
    :param test_pause_at_timestamp: The timestamp to which normal execution should run
    before pausing.
    """

    mapped_tag_id = endtoend.testhandle.schedule_tag_ids_to_e2e_ids[schedule_tag_id]

    endtoend.core_api_helper.update_account_schedule_tag(
        account_schedule_tag_id=mapped_tag_id,
        test_pause_at_timestamp=test_pause_at_timestamp.isoformat(),
    )


def skip_scheduled_jobs_between_dates(
    schedule_tag_id: str,
    skip_start_date: datetime,
    skip_end_date: datetime,
):
    """
    Skips all schedule executions for given schedule tag between the provided dates. Execution will
    continue as normal after skipped dates up until the provided test_pause_at_timestamp.
    :param schedule_tag_id: The schedule tag ID as defined in the Smart Contract.
    :param skip_start_date: The timestamp from which to begin skipping execution.
    :param skip_end_date: The timestamp at which to stop skipping execution.
    :param test_pause_at_timestamp: The timestamp until which normal execution should
    continue after all schedules in the defined period have been skipped.
    """

    mapped_tag_id = endtoend.testhandle.schedule_tag_ids_to_e2e_ids[schedule_tag_id]

    endtoend.core_api_helper.update_account_schedule_tag(
        account_schedule_tag_id=mapped_tag_id,
        schedule_status_override_start_timestamp=skip_start_date.isoformat(),
        schedule_status_override_end_timestamp=skip_end_date.isoformat(),
        schedule_status_override="ACCOUNT_SCHEDULE_TAG_SCHEDULE_STATUS_OVERRIDE_TO_SKIPPED",
    )
