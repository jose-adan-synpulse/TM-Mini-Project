# standard libs
import re
import time
from typing import Dict
from unittest.mock import Mock


def replace_supervisee_version_ids_in_supervisor(
    supervisor_contract_code: str, supervisee_alias_to_version_id: Dict[str, str]
) -> str:
    """
    Due to the fact that supervisor contract currently has to hard code
    its supervisee product version ids, there is a need to search and
    replace them with the actual version ids uploaded into the environment
    both when deploying via CLU or during e2e tests;
    This helper provides that utility and is currently being used both in
    e2e and simulation tests
    Note that the regex takes into account CLU resource dependency syntax
    &{dependent_resource}, working examples can be found under:
    https://regex101.com/r/hx4MRL/1
    """
    if supervisee_alias_to_version_id is None:
        raise ValueError(
            "Must provide supervisee_alias_to_version_id for supervisor contract"
        )

    regex = r"(?P<alias_prefix>alias=[\'\"]?)(?P<alias>[\w\d]*)(?P<pvid_prefix>[\'\"]?,\s*smart_contract_version_id=[\'\"]?)(?P<pvid>[\w\d&{}]*)(?P<suffix>[\'\"]?)"  # noqa: E501

    def replace_ids(match: re.Match):
        supervisee_alias = match.group("alias")
        product_ver_id = supervisee_alias_to_version_id.get(supervisee_alias)
        if product_ver_id is None:
            raise NameError(
                f"Missing {supervisee_alias} in {supervisee_alias_to_version_id}"
            )
        # Defend against constant with no quotes
        separator = "'" if not match.group("suffix") else ""
        return (
            match.group("alias_prefix")
            + match.group("alias")
            + match.group("pvid_prefix")
            + separator
            + product_ver_id
            + match.group("suffix")
            + separator
        )

    return re.sub(pattern=regex, repl=replace_ids, string=supervisor_contract_code)


def create_mock_message_queue(
    sample_message_file: str,
    yield_message_range: int = 3,
    matched_message_sleep: int = 1,
    while_loop_sleep: int = 1,
) -> Mock:
    """
    This mocks a kafka consumer, returning a mocked poller
    Rather than overriding the poll function to a mock function, it is
    set to be a generator which is instantiated and then yields the
    desired responses for the kafka message queue mock.
    :param sample_message_file: file path to sample kafka message file
    :param yield_message_range: number of matched messages to return
    :param matched_message_sleep: sleep time between match messages
    :param while_loop_sleep: sleep time between None messages
    """
    with open(sample_message_file, encoding="utf-8") as file:
        sample_message = file.read()

    poller = Mock()
    message_response_mock = Mock()
    message_response_decode_mock = Mock()
    message_response_decode_mock.decode.return_value = sample_message
    message_response_mock.error.return_value = False
    message_response_mock.value.return_value = message_response_decode_mock

    def mock_message_queue():
        for _ in range(yield_message_range):
            yield message_response_mock
        time.sleep(matched_message_sleep)
        while True:
            # add sleep here to slow down infinite while loop
            time.sleep(while_loop_sleep)
            yield None

    poller.poll.side_effect = mock_message_queue()
    return poller
