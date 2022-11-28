# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
# standard libs
import logging
import os
import uuid
from functools import wraps
from typing import Dict, List, Optional, Union


# common
import common.test_utils.endtoend as endtoend
from common.kafka.kafka import (  # noqa: F401
    acked,
    initialise_consumer,
    initialise_producer,
    produce_message,
    subscribe_to_topics,
    wait_for_messages,
)

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Each test class execution defines a unique Kafka consumer group id.
# This prevents from missing any messages read by other test class executions
# that may be run at the same time.
DEFAULT_CONSUMER_CONFIG: Dict[str, Union[bool, int, str]] = {
    "group.id": "e2e_" + str(uuid.uuid4()),
}


class UnsupportedError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def kafka_only_helper(func):
    @wraps(func)
    def wrapper(*arg, **kwargs):
        if not endtoend.testhandle.use_kafka:
            raise UnsupportedError(f"{func.__name__}() requires Kafka to be enabled")
        return func(*arg, **kwargs)

    return wrapper


def kafka_only_test(func):
    @wraps(func)
    def wrapper(self):
        if not endtoend.testhandle.use_kafka:
            self.skipTest("Kafka is required to run this test")
        return func(self)

    return wrapper


def initialise_all_consumers(
    topics: List[str],
    consumer_config: Optional[Dict[str, Union[str, bool, int]]] = None,
):
    """
    Initialises consumers for required topics
    :param topic: list[str], List of Kafka topics to subsscribe to
    """

    config = DEFAULT_CONSUMER_CONFIG.copy()
    config.update(consumer_config or {})

    # Consumers are initialised and destroyed at a test class level, so we should
    # only be initialising once for each topic
    endtoend.testhandle.kafka_consumers = subscribe_to_topics(
        topics=topics,
        consumer_config=config,
    )
