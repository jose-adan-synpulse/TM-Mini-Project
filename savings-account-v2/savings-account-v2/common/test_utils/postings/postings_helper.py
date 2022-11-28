# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
from datetime import datetime
import uuid
from typing import Dict, List, Union
from common.test_utils.postings.posting_classes import (
    PostingInstructionBatchEvent,
    PostingInstruction,
    Instruction,
)


def create_posting_instruction_batch(
    instructions: List[Instruction],
    value_datetime: Union[datetime, str] = None,
    client_transaction_id: str = None,
    override: bool = False,
    batch_details: str = None,
    instruction_details: str = None,
    client_batch_id: str = None,
    client_id: str = None,
) -> Dict[str, Dict]:
    """
    In the current implementation of Inception posting framework, there
    are three levels of objects to be instantiated which helps convert
    the data into a valid json payload for posting apis, they are:

    1. Instruction
    2. PostingInstruction
    3. PostingInstructionBatch

    this helper method takes in a list of Instruction objects, instantiates
    both PostingInsctruction and PostingInstructionBatch objects accordingly,
    then returns a valid Dict from the final PIB object to be used as payload.

    Certain values like client_id, client_transanction_id and client_batch_id all
    have standard default values as set in the corresponding class constructors.

    The output of this helper is valid in both e2e posting api calls as well as
    simulator events.

    As Instruction objects are mostly fungible, once instanciated, the same
    object instance can be used multiple times as input of this helper, as distinguishing
    factors like client_transaction_id and timestamps all sit outside of Instruction
    """
    client_batch_id = client_batch_id or str(uuid.uuid4())
    posting_instructions = [
        PostingInstruction(
            client_transaction_id=client_transaction_id,
            override=override,
            instruction=instruction,
            instruction_details=instruction_details,
        )
        for instruction in instructions
        if isinstance(instruction, Instruction)
    ]

    return create_pib_from_posting_instructions(
        posting_instructions=posting_instructions,
        value_datetime=value_datetime,
        batch_details=batch_details,
        client_batch_id=client_batch_id,
        client_id=client_id,
    )


def create_pib_from_posting_instructions(
    posting_instructions: List[PostingInstruction],
    value_datetime: Union[datetime, str] = None,
    batch_details: str = None,
    client_batch_id: str = None,
    client_id: str = None,
) -> Dict[str, Dict]:
    """
    Create a posting instruction batch from a list of multiple posting instructions
    Allowing each posting instruction to contain its own distinct instruction_details
    """
    client_batch_id = client_batch_id or str(uuid.uuid4())

    if value_datetime and isinstance(value_datetime, datetime):
        value_datetime = datetime.isoformat(value_datetime)

    return PostingInstructionBatchEvent(
        posting_instructions=posting_instructions,
        value_timestamp=value_datetime if value_datetime else None,
        client_batch_id=client_batch_id,
        batch_details=batch_details,
        client_id=client_id,
    ).to_dict()
