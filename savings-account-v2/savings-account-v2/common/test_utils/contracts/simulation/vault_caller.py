# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
# standard libs
import functools
import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from collections import namedtuple
# third party
import requests

proxies = {
  'http': 'http://proxy.synpulse.com:3128',
  'https': 'https://proxy.synpulse.com:3128',
}
SimulationInstruction = namedtuple("SimulationInstruction", ["time", "instruction"])
# common
from common.python.file_utils import load_file_contents
from common.test_utils.contracts.simulation.helper import (
    account_to_simulate,
    create_flag_definition_event,
    create_derived_parameters_instructions,
    create_smart_contract_module_versions_link,
)
from common.test_utils.common.utils import (
    replace_supervisee_version_ids_in_supervisor,
)
from common.test_utils.contracts.simulation.data_objects.data_objects import (
    ContractConfig,
    SimulationEvent,
    SupervisorConfig,
    ContractModuleConfig,
)

_DEFAULT_OPS_AUTH_HEADER_NAME = "tm_ops_auth_token"
_TESTING_INTERNAL_ASSET_ACCOUNT_PATH = (
    "internal_accounts/" "testing_internal_asset_account_contract.py"
)
_TESTING_INTERNAL_LIABILITY_ACCOUNT_PATH = (
    "internal_accounts/" "testing_internal_liability_account_contract.py"
)


class AuthCookieNotFound(Exception):
    def __str__(self):
        return "Could not find the auth cookie with key %r" % self.args[0]


class VaultException(Exception):
    def __init__(self, vault_error_code, message):
        self.vault_error_code = vault_error_code
        self.message = message

    def __str__(self):
        return "An exception was raised inside Vault:\nError Code: %s\nMessage:\n%s" % (
            self.vault_error_code,
            self.message,
        )


def _auth_required(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        return func(self, *args, **kwargs)

    return wrapper


class Client:
    def __init__(self, *, core_api_url, auth_token, ops_auth_header_name=None):
        self._core_api_url = core_api_url.rstrip("/")
        self._auth_token = auth_token
        self._ops_auth_header_name = (
            ops_auth_header_name or _DEFAULT_OPS_AUTH_HEADER_NAME
        )
        """ Setting up proxy
        """
        session = requests.Session()
        session.proxies.update(proxies)
        session.get(core_api_url)
        self._session = session

        #self._session = requests.Session()
        self._set_session_headers()

    @_auth_required
    def _api_post(self, url, payload, timeout, debug=False):
        #Uncomment for POSTMAN Body Payload
        # with open("payload.json", "w") as f:
        #     f.write(json.dumps(payload))
        response = self._session.post(
            self._core_api_url + url,
            headers={"grpc-timeout": timeout},
            json=payload,
            stream=debug,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            return self._handle_error(response, e)

        try:
            data = []
            # The response for this endpoint is streamed as new line separated JSON.
            for line in response.iter_lines():
                line_json = json.loads(line)
                data.append(line_json)
                if debug:
                    print(line_json)
                if line_json.get("error"):
                    return self._raise_error(line)
            return data
        except requests.exceptions.HTTPError as e:
            return self._handle_error(response, e)

    @staticmethod
    def _handle_error(response, e):
        try:
            content = json.loads(response.text)
        except json.decoder.JSONDecodeError as json_e:
            raise json_e from e

        if "vault_error_code" in content and "message" in content:
            raise VaultException(content["vault_error_code"], content["message"]) from e

        if "error" in content:
            raise ValueError(content["error"]) from e

        raise e

    @staticmethod
    def _raise_error(response):
        try:
            content = json.loads(response)
        except json.decoder.JSONDecodeError as json_e:
            raise json_e

        if "error" in content:
            raise ValueError(content["error"])

    def _set_session_headers(self):
        headers = {"X-Auth-Token": self._auth_token, "Content-Type": "application/json"}
        self._session.headers = headers

    def simulate_contracts(
        self, *, smart_contracts, start_timestamp, end_timestamp, instructions, timeout="10S"
    ):
        instructions = [_instruction_to_json(instruction) for instruction in instructions]
        vardata = {
            "smart_contracts": smart_contracts,

            "start_timestamp": _datetime_to_rfc_3339(start_timestamp),

            "end_timestamp": _datetime_to_rfc_3339(end_timestamp),

            "instructions": instructions,
            }
        #Uncomment for POSTMAN Body Payload
        # with open("payload.json", "w") as f:
        #     f.write(json.dumps(vardata))
        # print(vardata)
        payload = self._api_post(
            "/v1/contracts:simulate",
            {
                "smart_contracts": smart_contracts,
                "start_timestamp": _datetime_to_rfc_3339(start_timestamp),
                "end_timestamp": _datetime_to_rfc_3339(end_timestamp),
                "instructions": instructions,
            },
            timeout=timeout,
        )
        return payload

    

    def simulate_smart_contract(
        self,
        start_timestamp: datetime,
        end_timestamp: datetime,
        events: List[SimulationEvent],
        timeout: str = "360S",
        supervisor_contract_code: str = None,
        supervisor_contract_version_id: str = None,
        supervisee_alias_to_version_id: Dict[str, str] = None,
        contract_codes: List[str] = None,
        smart_contract_version_ids: List[str] = None,
        templates_parameters: List[Dict[str, str]] = None,
        contract_config: Optional[ContractConfig] = None,
        supervisor_contract_config: SupervisorConfig = None,
        account_creation_events: List[Dict[str, Any]] = None,
        internal_account_ids: List[str] = None,
        flag_definition_ids: List[str] = None,
        output_account_ids: List[str] = None,
        output_timestamps: List[datetime] = None,
        debug: bool = False,
    ):

        internal_account_creation_events = []
        account_creation_events = account_creation_events or []
        default_events = []
        contract_codes = contract_codes or []
        smart_contract_version_ids = smart_contract_version_ids or []
        templates_parameters = templates_parameters or []
        flag_definition_ids = flag_definition_ids or []
        internal_account_ids = internal_account_ids or []
        contract_modules_to_simulate = []

        if internal_account_ids:
            for internal_account_id in internal_account_ids:
                # internal_account_ids is either a list of ids (in which case the accounts will be
                # instantiated as liability accounts or a dict with id:tside key-value pairs
                if isinstance(internal_account_ids, dict):
                    tside = internal_account_ids.get(internal_account_id, "LIABILITY")
                else:
                    tside = "LIABILITY"
                contract_file_path = (
                    _TESTING_INTERNAL_ASSET_ACCOUNT_PATH
                    if tside == "ASSET"
                    else _TESTING_INTERNAL_LIABILITY_ACCOUNT_PATH
                )
                internal_account = account_to_simulate(
                    timestamp=start_timestamp,
                    account_id=internal_account_id,
                    contract_file_path=contract_file_path,
                )
                internal_account_creation_events.append(internal_account)

        # putting internal account creation events at the front to ensure all events are in
        # chronological order, as mandated by the simulator endpoint
        account_creation_events = (
            internal_account_creation_events + account_creation_events
        )

        if flag_definition_ids:
            for flag_definition_id in flag_definition_ids:
                flag_definition_event = create_flag_definition_event(
                    timestamp=start_timestamp, flag_definition_id=flag_definition_id
                )
                default_events.append(flag_definition_event)

        for account in account_creation_events:
            contract_codes.append(account["contract_file_contents"])
            templates_parameters.append(account["template_parameters"])
            smart_contract_version_ids.append(account["smart_contract_version_id"])
            if account["event"]:
                default_events.append(account["event"])

        if supervisor_contract_code is not None:
            supervisor_contract_code = replace_supervisee_version_ids_in_supervisor(
                supervisor_contract_code, supervisee_alias_to_version_id
            )

        contract_configs = (
            [contract_config]
            if contract_config
            else (
                supervisor_contract_config.supervisee_contracts
                if supervisor_contract_config
                else []
            )
        )

        (
            contract_module_linking_events,
            contract_modules_to_simulate,
        ) = _create_smart_contract_module_links(start_timestamp, contract_configs)
        default_events.extend(contract_module_linking_events)

        payload = self._api_post(
            "/v1/contracts:simulate",
            {
                "start_timestamp": _datetime_to_rfc_3339(start_timestamp),
                "end_timestamp": _datetime_to_rfc_3339(end_timestamp),
                "smart_contracts": _smart_contract_to_json(
                    contract_codes, templates_parameters, smart_contract_version_ids
                ),
                "supervisor_contracts": _supervisor_contract_to_json(
                    supervisor_contract_code, supervisor_contract_version_id
                ),
                "contract_modules": contract_modules_to_simulate,
                "instructions": [
                    _event_to_json(event) for event in default_events + events
                ],
                "outputs": create_derived_parameters_instructions(
                    output_account_ids, output_timestamps
                ),
            },
            timeout=timeout,
            debug=debug,
        )
        return payload


def _datetime_to_rfc_3339(dt):
    timezone_aware = dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None

    if not timezone_aware:
        raise ValueError("The datetime object passed in is not timezone-aware")

    return dt.astimezone().isoformat()


def _instruction_to_json(instruction):
    return {"timestamp": _datetime_to_rfc_3339(instruction.time), **instruction.instruction}

def _event_to_json(event):
    instruction = {
        "timestamp": _datetime_to_rfc_3339(event.time),
    }

    for key in event.event:
        instruction[key] = event.event[key]

    return instruction


def _smart_contract_to_json(
    contract_codes: List[str],
    templates_parameters: List[Dict[str, str]],
    smart_contract_version_ids: List[str],
) -> List:
    return [
        {
            "code": code,
            "smart_contract_param_vals": template_parameter,
            "smart_contract_version_id": smart_contract_version_id,
        }
        for code, template_parameter, smart_contract_version_id in zip(
            contract_codes, templates_parameters, smart_contract_version_ids
        )
    ]


def _supervisor_contract_to_json(
    supervisor_contract_code, supervisor_contract_version_id
):
    """
    Helper that support the supervisor contract object. Although this is a list field,
    only one supervisor contract can currently be simulated for each request.
    :param supervisor_contract_code: Source code of the supervisor contract that is to be simulated.
    :param supervisor_contract_version_id: The ID that will be used as the supervisor contract
        version ID in the simulation and can be referenced by the simulation instructions.
    :return: a hypothetical list of supervisor contracts to simulate.
    """
    supervisor_contracts = []
    if supervisor_contract_code is not None:
        supervisor_contracts.append(
            {
                "code": supervisor_contract_code,
                "supervisor_contract_version_id": supervisor_contract_version_id,
            }
        )
    return supervisor_contracts


def _create_smart_contract_module_links(
    start: datetime, contract_configs: List[ContractConfig]
) -> Tuple[List[SimulationEvent], List[Dict[str, str]]]:

    events = []
    contract_modules_to_simulate = []
    existing_contract_modules = []
    for contract_config in contract_configs:
        alias_to_sc_version_id = {}
        if contract_config.linked_contract_modules:
            for contract_module in contract_config.linked_contract_modules:

                existing_module_version_id = get_existing_module_version_id(
                    contract_module, existing_contract_modules
                )
                if existing_module_version_id is not None:
                    contract_module_version_id = existing_module_version_id

                else:
                    contract_module_code = load_file_contents(contract_module.file_path)
                    contract_module_version_id = str(uuid.uuid4())
                    contract_module.version_id = contract_module_version_id

                    existing_contract_modules.append(contract_module)

                    details = {
                        "code": contract_module_code,
                        "contract_module_version_id": contract_module_version_id,
                    }
                    contract_modules_to_simulate.append(details)

                alias_to_sc_version_id[
                    contract_module.alias
                ] = contract_module_version_id

            aliases_as_str = "_".join(alias_to_sc_version_id.keys())
            events.append(
                create_smart_contract_module_versions_link(
                    timestamp=start,
                    link_id=f"sim_link_modules_{aliases_as_str}_with_contract_"
                    f"{contract_config.smart_contract_version_id}",
                    smart_contract_version_id=contract_config.smart_contract_version_id,
                    alias_to_contract_module_version_id=alias_to_sc_version_id,
                )
            )
    return events, contract_modules_to_simulate


def get_existing_module_version_id(
    contract_module: ContractModuleConfig,
    existing_contract_modules: List[ContractModuleConfig],
):
    for existing_module in existing_contract_modules:
        if contract_module.file_path == existing_module.file_path:
            return existing_module.version_id
