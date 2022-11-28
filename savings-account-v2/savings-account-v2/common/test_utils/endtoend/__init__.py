# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# flake8: noqa
# standard libs
import argparse
import functools
import logging
import os
import signal
import sys
import unittest
from typing import Dict, List, Tuple

# common
import common.test_utils.endtoend.accounts_helper as accounts_helper
import common.test_utils.endtoend.balances as balances_helper
import common.test_utils.endtoend.contracts_helper as contracts_helper
import common.test_utils.endtoend.contract_modules_helper as contract_modules_helper
import common.test_utils.endtoend.core_api_helper as core_api_helper
import common.test_utils.endtoend.data_loader_helper as data_loader_helper
import common.test_utils.endtoend.helper as helper
import common.test_utils.endtoend.kafka_helper as kafka_helper
import common.test_utils.endtoend.postings as postings_helper
import common.test_utils.endtoend.supervisors_helper as supervisors_helper
import common.test_utils.endtoend.workflows_helper as workflows_helper
import common.test_utils.endtoend.workflows_api_helper as workflows_api_helper
import common.test_utils.endtoend.xpl_helper as xpl_helper
import common.test_utils.endtoend.schedule_helper as schedule_helper
import common.test_utils.endtoend as endtoend

log = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

testhandle = endtoend.helper.TestInstance()


def sig_handler(sig, frame):
    endtoend.helper.teardown()
    raise KeyboardInterrupt


signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGTERM, sig_handler)


KAFKA_TOPICS = [
    postings_helper.POSTINGS_API_RESPONSE_TOPIC,
    accounts_helper.ACCOUNT_UPDATE_EVENTS_TOPIC,
    supervisors_helper.PLAN_UPDATE_EVENTS_TOPIC,
    balances_helper.ACCOUNT_BALANCE_EVENTS_TOPIC,
]


def kafka_setup(topics: List[str]) -> None:
    try:
        if endtoend.testhandle.use_kafka != True:
            raise Exception(
                "To enable Kafka, please set the TestInstance.use_kafka flag to"
                "True. This can be done by passing the command line argument "
                "'--use_kafka'."
            )

        kafka_config = endtoend.testhandle.available_environments[
            endtoend.testhandle.environment
        ].kafka_config

        # Initialise consumers
        kafka_helper.initialise_all_consumers(topics, kafka_config)

        # Initialise producer
        endtoend.testhandle.kafka_producer = endtoend.kafka_helper.initialise_producer(
            kafka_config
        )

    except KeyError:
        log.exception('Does the environment config contain an entry for "kafka"?')
        endtoend.helper.teardown()
        raise
    except:
        endtoend.helper.teardown()
        raise


def standard_setup():
    try:
        endtoend.helper.setup_environments()
        endtoend.contracts_helper.create_account_schedule_tags(
            testhandle.ACCOUNT_SCHEDULE_TAGS
        )
        endtoend.contracts_helper.create_flag_definitions(testhandle.FLAG_DEFINITIONS)
        endtoend.contracts_helper.create_calendars(testhandle.CALENDARS)
        endtoend.workflows_helper.create_workflow_definition_id_mapping()
        endtoend.contracts_helper.create_required_internal_accounts(
            testhandle.TSIDE_TO_INTERNAL_ACCOUNT_ID
        )
        endtoend.contracts_helper.upload_contracts(testhandle.CONTRACTS)
        endtoend.supervisors_helper.upload_supervisor_contracts(
            supervisor_contracts=testhandle.SUPERVISORCONTRACTS
        )
        endtoend.contract_modules_helper.upload_contract_modules(
            testhandle.CONTRACT_MODULES
        )
        endtoend.workflows_helper.update_and_upload_workflows()
        endtoend.core_api_helper.init_postings_api_client(
            client_id=postings_helper.POSTINGS_API_CLIENT_ID,
            response_topic=postings_helper.POSTINGS_API_RESPONSE_TOPIC,
        )
    except:
        endtoend.helper.teardown()
        raise


def extract_args() -> Tuple[argparse.Namespace, List[str]]:
    """
    extract e2e framework args from the command line
    :return: a Namespace containing the parsed known arguments and a List of unrecognised
    arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--use_kafka",
        action="store",
        default=True,
        type=lambda x: x.lower() == "true",
    )
    parser.add_argument(
        "--environment",
        required=False,
        action="store",
        type=str,
        help=f"Environment to run the test against",
    )
    parser.add_argument(
        "--instances",
        required=False,
        action="store",
        type=int,
        help=f"Number of accounts to create for performance schedules test (overrides any value in '_profile.yaml')",
    )
    # ignore unknown args, such as test names if invoking via python3 -m unittest <test_module>
    # by passing sys.argv we ensure that the module path is returned in unknown_args. This allows
    # us to easily remove the --use_kafka flag from the list, regardless of its value
    return parser.parse_known_args(sys.argv)


class End2Endtest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        # Ensure we can see full details of assertion failures
        cls.maxDiff = None

        standard_setup()
        args, _ = extract_args()
        endtoend.testhandle.use_kafka = args.use_kafka

        # These statements cannot be merged as use_kafka may have been
        # initialised elsewhere
        if endtoend.testhandle.use_kafka:
            kafka_setup(KAFKA_TOPICS)

    @classmethod
    def tearDownClass(cls):
        endtoend.helper.teardown()
        if endtoend.testhandle.use_kafka:
            for consumer in endtoend.testhandle.kafka_consumers.values():
                consumer.close()
            endtoend.testhandle.kafka_consumers = {}


class AcceleratedEnd2EndTest(unittest.TestCase):
    default_tags: Dict[str, str]
    paused_tags: Dict[str, Dict[str, str]]

    def __init__(self, methodName: str = "runTest") -> None:
        endtoend.testhandle.is_accelerated_test = True
        endtoend.testhandle.use_kafka = True
        super().__init__(methodName=methodName)

    class Decorators(object):
        @classmethod
        def set_paused_tags(cls, paused_tags):
            """
            Decorator that allows each test to easily define which schedules will be paused
            """

            def test_decorator(function):
                @functools.wraps(function)
                def wrapper(test, *args, **kwargs):
                    test.paused_tags = paused_tags
                    endtoend.testhandle.ACCOUNT_SCHEDULE_TAGS = test.default_tags.copy()
                    endtoend.testhandle.ACCOUNT_SCHEDULE_TAGS.update(
                        {
                            tag: details["tag_resource"]
                            for tag, details in paused_tags.items()
                        }
                    )
                    log.debug(endtoend.testhandle.ACCOUNT_SCHEDULE_TAGS)
                    function(test, *args, **kwargs)

                return wrapper

            return test_decorator

    @classmethod
    def setUpClass(cls) -> None:
        # Ensure we can see full details of assertion failures
        cls.maxDiff = None

        # TODO: this could be improved to avoid reloading environments again when
        # the individual tests use standard_setup()
        endtoend.helper.setup_environments()
        kafka_setup(KAFKA_TOPICS)

    @classmethod
    def tearDownClass(cls):
        endtoend.helper.teardown()

    def tearDown(self) -> None:
        # Don't accidentally re-use tags if two tests refer to the same test resources
        for tag_id in self.paused_tags:
            endtoend.testhandle.schedule_tag_ids_to_e2e_ids.pop(tag_id)
            endtoend.testhandle.schedule_tag_file_paths_to_e2e_ids.pop(
                endtoend.testhandle.ACCOUNT_SCHEDULE_TAGS[tag_id]
            )


def runtests() -> None:
    # unknown_args will not contain any e2e framework args that could trip up unittest
    args, unknown_args = extract_args()
    endtoend.testhandle.use_kafka = args.use_kafka

    tests = [a for a in unknown_args[1:] if a.startswith("test")]
    if len(tests) > 0:
        for test in tests:
            for i in range(len(End2Endtest.__subclasses__())):
                module = End2Endtest.__subclasses__()[i]
                if test in dir(module):
                    unittest.main(module=module, defaultTest=test, argv=unknown_args)
    else:
        unittest.main(argv=unknown_args)
