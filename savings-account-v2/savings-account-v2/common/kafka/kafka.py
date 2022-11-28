# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
# standard libs
import json
import logging
import os
import queue
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Union

# third party
from confluent_kafka import Consumer, KafkaError, Producer

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DEFAULT_PRODUCER_CONFIG: Dict[str, Union[bool, int, str]] = {
    "linger.ms": 500,
}

# These are similar to Vault platform kafka settings, with the exception of group.id,
# enable.auto.commit and the custom logger
DEFAULT_CONSUMER_CONFIG: Dict[str, Union[bool, int, str]] = {
    "group.id": str(uuid.uuid4()),
    "enable.auto.commit": True,
    "metadata.request.timeout.ms": 20000,
    "api.version.request": True,
    # Optimise the consumers for low latency.  If we find we
    # need super-high throughput we can introduce a separate
    # factory function for that.
    "fetch.wait.max.ms": 100,
    "log.connection.close": False,
    # Max number of bytes per partition returned by the server
    "max.partition.fetch.bytes": 1024 * 1024 * 5,
    "statistics.interval.ms": 15000,
    # This is per partition. The default buffers 1GB which can easily
    # jump over the container mem limits if the consumer starts lagging
    # behind
    "queued.max.messages.kbytes": 1024 * 32,
    "socket.keepalive.enable": True,
    "max.poll.interval.ms": "86400000",
    # Under heavy load the heartbeats sometimes go missing and consumers get removed from the group
    # Higher timeout makes this much less likely without many side-effects as we only have one
    # consumer per group anyway
    "session.timeout.ms": "100000",
    # 'debug': 'all'
}


class UnsupportedError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def initialise_consumer(
    config: Optional[Dict[str, Union[bool, int, str]]] = None,
) -> Consumer:
    """
    Initialises a kafka consumer relying on default config that can be overriden
    :param config: if specified, the default config is updated with the contents of this dictionary
     This means it takes precedence over any default config or other parameters
    :return: the initialised kafka consumer
    """

    final_config = DEFAULT_CONSUMER_CONFIG.copy()
    final_config.update(config or {})

    return Consumer(final_config)


def initialise_producer(
    config: Optional[Dict[str, Union[bool, int, str]]] = None,
) -> Producer:
    """
    Initialises a kafka producer relying on default config that can be overriden
    :param kafka_config: the kafka config to use. If the KafkaConfig.config is specified,
     the default config is updated with the contents of this dictionary. This means it takes
     precedence over any default config or other parameters
    :param client_id: the client id that the producer will use
    :return: the initialised kafka producer
    """

    final_config = DEFAULT_PRODUCER_CONFIG.copy()
    if config:
        final_config.update(config)

    return Producer(final_config)


def wait_for_messages(
    consumer: Consumer,
    matcher: Callable,
    callback: Optional[Callable],
    unique_message_ids: Dict[str, Any],
    inter_message_timeout: int = 30,
    matched_message_timeout: int = 30,
) -> Dict[str, Any]:
    """
    Using the consumer, poll the topic for any matched messages.
    :param consumer: a Kafka topic consumer
    :param matcher: a callable used to determine if any messages received by the consumer are valid
    messages. This method must return a tuple (str, str, bool). The first str is the resulting
    matched event_id, the second str is the matched message unique request id and is used for
    idempotency and the bool is whether the message is matched or not.
    :param callback: called when a message has been matched
    :param unique_message_ids: Dict of unique message ids passed to the matcher,
    e.g. Dict of account ids. The value can be used to hold any additional information to
    be manipulated in either the matcher or callback functions, otherwise a dummy value (None)
    should be provided.
    :param inter_message_timeout: a maximum time to wait between receiving any messages from the
    consumer (0 for no timeout)
    :param matched_message_timeout: a maximum time to wait between receiving matched messages from
    the consumer (0 for no timeout)
    :return: Dict of message ids that failed to match. This is the exact same data structure
    as message_ids
    """
    last_message_time = time.time()
    last_matched_message_time = time.time()
    seen_matched_message_requests = set()

    while len(unique_message_ids) > 0:
        msg = consumer.poll(0.1)
        if matched_message_timeout:
            delay = time.time() - last_matched_message_time
            if delay > matched_message_timeout:
                log.warning(
                    f"Waited {delay:.1f}s since last matched message received. "
                    f"Timeout set to {matched_message_timeout:.1f}. Exiting "
                    f"after {len(seen_matched_message_requests)} "
                    f"messages received"
                )
                break
        if msg is None:
            if inter_message_timeout:
                delay = time.time() - last_message_time
                if delay > inter_message_timeout:
                    log.warning(
                        f"Waited {delay:.1f}s since last message received. "
                        f"Timeout set to {inter_message_timeout:.1f}. Exiting "
                        f"after {len(seen_matched_message_requests)} "
                        f"messages received"
                    )
                    break
        else:
            last_message_time = time.time()
            if not msg.error():
                event_msg = json.loads(msg.value().decode())
                (
                    event_id,
                    event_request_id,
                    is_matched,
                ) = matcher(event_msg, unique_message_ids)
                if is_matched and event_request_id not in seen_matched_message_requests:
                    last_matched_message_time = time.time()
                    seen_matched_message_requests.add(event_request_id)

                    if event_id:
                        del unique_message_ids[event_id]
                    if callback:
                        callback(event_msg)
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                log.error(
                    "End of partition reached {0}/{1}".format(
                        msg.topic(), msg.partition()
                    )
                )
            else:
                log.error("Error occured: {0}".format(msg.error().str()))

    return unique_message_ids


def acked(err, msg):
    if err is not None:
        log.exception(f"Failed to deliver message: {msg.value()}: {err.str()}")
    else:
        log.debug("Message produced: {0}".format(msg.value()))


def produce_message(
    producer: Producer,
    topic: str,
    message: str,
    key: Optional[str] = None,
    on_delivery: Callable = acked,
):

    producer.produce(topic=topic, key=key, value=message, on_delivery=on_delivery)
    producer.poll(0)


def subscribe_to_topics(
    topics: List[str],
    consumer_config: Optional[Dict[str, Union[str, bool, int]]] = None,
) -> Dict[str, Consumer]:
    """
    Initialises consumers for required topics, waiting for queue assignment before returning
    :param topic: List of Kafka topics to subscribe to
    :param consumer_config: Consumer config to override any defaults
    :return: Dict of topic to initialised consumer
    """

    consumers = {}
    assign_queue = queue.Queue()

    def assign_cb(consumer, partitions):
        assign_queue.get()
        assign_queue.task_done()

    log.info(f"Subscribing to {len(topics)} consumers...")
    for topic in topics:
        consumers[topic] = initialise_consumer(config=consumer_config)
        assign_queue.put(topic, block=False)
        consumers[topic].subscribe([topic], on_assign=assign_cb)

    # Wait until all new consumers have been assigned partitions. As we use latest
    # auto.offset.reset, messages produced before consumer readiness could otherwise be missed
    while not assign_queue.empty():
        for consumer in consumers.values():
            consumer.poll(0.1)

    return consumers
