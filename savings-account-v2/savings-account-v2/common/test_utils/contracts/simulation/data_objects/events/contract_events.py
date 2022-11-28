# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
from typing import Dict
from dataclasses import dataclass


@dataclass
class CreateSmartContractModuleVersionsLink:
    """
    This class represents a Create Smart Contract Module Versions Link event that can be consumed
    by the Simulation endpoint to instruct the creation of a link between a smart contract and a
    contract module.
    Please note that `.to_dict()` must be called when the object is passed into vault_caller.

    Args:
        param id: id of the Link
        param smart_contract_version_id: The smart contract version ID this link relates to
        param alias_to_contract_module_version_id: Map of alias -> ContractModuleVersionID
            containing all contract module versions that should be linked to the smart contract
            version ID
    """

    id: str
    smart_contract_version_id: str
    alias_to_contract_module_version_id: Dict

    def to_dict(self):
        return {"create_smart_contract_module_versions_link": self.__dict__}
