# Copyright @ 2020-2021 Thought Machine Group Limited. All rights reserved.
"""
This module exposes easy helper methods for sending each type
of posting instruction and settings flags with minimal effort from the test writer.
However, for posting instruction types these helper methods are kept simple and minimal on purpose,
only exposing a small subset of fields available on each posting instruction
type.
Any more complex Posting Instruction Batches should be constructed by the
test writer out of the base available objects.
"""
from common.test_utils.contracts.simulation.data_objects.data_objects import (
    SimulationEvent,
)
from common.test_utils.postings.posting_classes import (
    Instruction,
    AuthorisationAdjustment,
    CustomInstruction,
    InboundAuthorisation,
    InboundHardSettlement,
    OutboundAuthorisation,
    OutboundHardSettlement,
    Posting,
    Release,
    Settlement,
    Transfer,
)
from common.test_utils.postings.postings_helper import create_posting_instruction_batch

from datetime import datetime
from typing import Dict, Optional, List


def create_auth_adjustment_event(
    amount: str,
    value_datetime: datetime,
    client_transaction_id: str = None,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    client_batch_id: Optional[str] = None,
    pib_timestamp: Optional[datetime] = None,
) -> SimulationEvent:
    """
    Returns a SimulationEvent containing a Posting Instruction Batch
    instructing an Authorisation Adjustment
    :param amount: string representation of the amount to be sent.
    :param value_datetime: the datetime at which the Posting Instruction Batch
                           will be applied in the simulation.
    :param client_transaction_id: the ID of the client transaction this posting
                                  instruction is creating or mutating.
    :param instruction_details: An optional mapping containing instruction-level metadata.
    :param batch_details: A dictionary containing batch level metadata.
    :param client_batch_id: An id which allows related PostingInstructions
                            to be associated with each other.
    :param pib_timestamp: Optional value timestamp at which the Posting Instruction Batch
                          will be applied in the simulation. If "None" defaults to value_datetime.
    :return SimulationEvent with a AuthorisationAdjustment Posting Instruction Batch
    """
    auth_adjustment_instruction = AuthorisationAdjustment(amount=amount)
    return create_posting_instruction_batch_event(
        instructions=[auth_adjustment_instruction],
        value_datetime=value_datetime,
        client_transaction_id=client_transaction_id,
        instruction_details=instruction_details,
        pib_timestamp=pib_timestamp,
        batch_details=batch_details,
        client_batch_id=client_batch_id,
    )


def create_custom_instruction_event(
    amount: str,
    debtor_target_account_id: str,
    creditor_target_account_id: str,
    debtor_target_account_address: str,
    creditor_target_account_address: str,
    value_datetime: datetime,
    client_transaction_id: Optional[str] = None,
    denomination: Optional[str] = None,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    client_batch_id: Optional[str] = None,
    pib_timestamp: Optional[datetime] = None,
) -> SimulationEvent:
    """
    Returns a SimulationEvent containing a Posting Instruction Batch
    instructing a Custom Instruction
    :param amount string representation of the amount to be sent
    :param debtor_target_account_id account from which the money should be debited
    :param creditor_target_account_id account to which the money should be credited to
    :param debtor_target_account_address address from which the money should be debited
    :param creditor_target_account_address address to which the money should be credited to
    :param value_datetime the datetime at which the Posting Instruction Batch
           will be applied in the simulation
    :param denomination: the denomination the posting instruction will be in.
    :param instruction_details: An optional mapping containing instruction-level metadata.
    :param batch_details: A dictionary containing batch level metadata.
    :param client_batch_id: An id which allows related PostingInstructions
                            to be associated with each other.
    :param pib_timestamp: Optional value timestamp at which the Posting Instruction Batch
                          will be applied in the simulation. If "None" defaults to value_datetime.
    :return SimulationEvent with a InboundHardSettlement Posting Instruction Batch
    """
    custom_instruction = CustomInstruction(
        postings=[
            Posting(
                account_id=debtor_target_account_id,
                amount=amount,
                credit=False,
                denomination=denomination,
                account_address=debtor_target_account_address,
            ),
            Posting(
                account_id=creditor_target_account_id,
                amount=amount,
                credit=True,
                denomination=denomination,
                account_address=creditor_target_account_address,
            ),
        ]
    )
    return create_posting_instruction_batch_event(
        instructions=[custom_instruction],
        value_datetime=value_datetime,
        client_transaction_id=client_transaction_id,
        instruction_details=instruction_details,
        pib_timestamp=pib_timestamp,
        batch_details=batch_details,
        client_batch_id=client_batch_id,
    )


def create_inbound_authorisation_event(
    amount: str,
    value_datetime: datetime,
    denomination: Optional[str] = None,
    client_transaction_id: Optional[str] = None,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    client_batch_id: Optional[str] = None,
    target_account_id: Optional[str] = None,
    internal_account_id: Optional[str] = None,
    pib_timestamp: Optional[datetime] = None,
) -> SimulationEvent:
    """
    Returns a SimulationEvent containing a Posting Instruction Batch
    instructing an Inbound Authorisation
    :param amount: string representation of the amount to be sent.
    :param value_datetime: the datetime at which the Posting Instruction Batch
                           will be applied in the simulation.
    :param denomination: the denomination the posting instruction will be in.
    :param client_transaction_id: the ID of the client transaction this posting
                                  instruction is creating or mutating.
    :param instruction_details: An optional mapping containing instruction-level metadata.
    :param batch_details: A dictionary containing batch level metadata.
    :param client_batch_id: An id which allows related PostingInstructions
                            to be associated with each other.
    :param target_account_id: Target customer account id for posting instruction
    :param internal_account_id: internal account id for posting instruction
    :param pib_timestamp: Optional value timestamp at which the Posting Instruction Batch
                          will be applied in the simulation. If "None" defaults to value_datetime.
    :return: SimulationEvent with a InboundAuthorisation Posting Instruction Batch
    """
    inbound_auth_instruction = InboundAuthorisation(
        target_account_id=target_account_id,
        internal_account_id=internal_account_id,
        amount=amount,
        denomination=denomination,
    )
    return create_posting_instruction_batch_event(
        instructions=[inbound_auth_instruction],
        value_datetime=value_datetime,
        client_transaction_id=client_transaction_id,
        instruction_details=instruction_details,
        pib_timestamp=pib_timestamp,
        batch_details=batch_details,
        client_batch_id=client_batch_id,
    )


def create_inbound_hard_settlement_event(
    amount: str,
    value_datetime: datetime,
    denomination: Optional[str] = None,
    client_transaction_id: Optional[str] = None,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    client_batch_id: Optional[str] = None,
    target_account_id: Optional[str] = None,
    internal_account_id: Optional[str] = None,
    pib_timestamp: Optional[datetime] = None,
) -> SimulationEvent:
    """
    Returns a SimulationEvent containing a Posting Instruction Batch
    instructing an Inbound Hard Settlement.
    :param amount: string representation of the amount to be sent.
    :param value_datetime: the datetime at which the event will be applied in the simulation.
    :param denomination: the denomination the posting instruction will be in.
    :param client_transaction_id: the ID of the client transaction this posting
                                  instruction is creating or mutating.
    :param instruction_details: An optional mapping containing instruction-level metadata.
    :param batch_details: A dictionary containing batch level metadata.
    :param client_batch_id: An id which allows related PostingInstructions
                            to be associated with each other.
    :param target_account_id: Target customer account id for posting instruction
    :param internal_account_id: internal account id for posting instruction
    :param pib_timestamp: Optional value timestamp at which the Posting Instruction Batch
                          will be applied in the simulation. If "None" defaults to value_datetime.
    :return: SimulationEvent with a InboundHardSettlement Posting Instruction Batch
    """
    inbound_hard_settlement_instruction = InboundHardSettlement(
        target_account_id=target_account_id,
        internal_account_id=internal_account_id,
        amount=amount,
        denomination=denomination,
    )
    return create_posting_instruction_batch_event(
        instructions=[inbound_hard_settlement_instruction],
        value_datetime=value_datetime,
        instruction_details=instruction_details,
        client_transaction_id=client_transaction_id,
        pib_timestamp=pib_timestamp,
        batch_details=batch_details,
        client_batch_id=client_batch_id,
    )


def create_outbound_authorisation_event(
    amount: str,
    value_datetime: datetime,
    denomination: Optional[str] = None,
    client_transaction_id: Optional[str] = None,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    client_batch_id: Optional[str] = None,
    target_account_id: Optional[str] = None,
    internal_account_id: Optional[str] = None,
    pib_timestamp: Optional[datetime] = None,
) -> SimulationEvent:
    """
    Returns a SimulationEvent containing a Posting Instruction Batch
    instructing an Outbound Authorisation
    :param amount: string representation of the amount to be sent.
    :param value_datetime: the datetime at which the Posting Instruction Batch
                           will be applied in the simulation.
    :param denomination: the denomination the posting instruction will be in.
    :param client_transaction_id: the ID of the client transaction this posting
                                  instruction is creating or mutating.
    :param instruction_details: An optional mapping containing instruction-level metadata.
    :param batch_details: A dictionary containing batch level metadata.
    :param client_batch_id: An id which allows related PostingInstructions
                            to be associated with each other.
    :param target_account_id: Target customer account id for posting instruction
    :param internal_account_id: internal account id for posting instruction
    :param pib_timestamp: Optional value timestamp at which the Posting Instruction Batch
                          will be applied in the simulation. If "None" defaults to value_datetime.
    :return: SimulationEvent with a OutboundAuthorisation Posting Instruction Batch
    """
    outbound_auth_instruction = OutboundAuthorisation(
        target_account_id=target_account_id,
        internal_account_id=internal_account_id,
        amount=amount,
        denomination=denomination,
    )
    return create_posting_instruction_batch_event(
        instructions=[outbound_auth_instruction],
        value_datetime=value_datetime,
        client_transaction_id=client_transaction_id,
        instruction_details=instruction_details,
        pib_timestamp=pib_timestamp,
        batch_details=batch_details,
        client_batch_id=client_batch_id,
    )


def create_outbound_hard_settlement_event(
    amount: str,
    value_datetime: datetime,
    denomination: Optional[str] = None,
    client_transaction_id: Optional[str] = None,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    client_batch_id: Optional[str] = None,
    target_account_id: Optional[str] = None,
    internal_account_id: Optional[str] = None,
    pib_timestamp: Optional[datetime] = None,
    advice: bool = None,
) -> SimulationEvent:
    """
    Returns a SimulationEvent containing a Posting Instruction Batch
    instructing an Outbound Hard Settlement
    :param amount: string representation of the amount to be sent.
    :param value_datetime: the datetime at which the event will be applied in the simulation.
    :param denomination: the denomination the posting instruction will be in.
    :param client_transaction_id: the ID of the client transaction this posting
                                  instruction is creating or mutating.
    :param instruction_details: An optional mapping containing instruction-level metadata.
    :param batch_details: A dictionary containing batch level metadata.
    :param client_batch_id: An id which allows related PostingInstructions
                            to be associated with each other.
    :param target_account_id: Target customer account id for posting instruction
    :param internal_account_id: internal account id for posting instruction
    :param pib_timestamp: Optional value timestamp at which the Posting Instruction Batch
                          will be applied in the simulation. If "None" defaults to value_datetime.
    :param advice: if true, the amount will be authorised regardless of balance check
    :return: SimulationEvent with a OutboundHardSettlement Posting Instruction Batch
    """
    outbound_hard_settlement_instruction = OutboundHardSettlement(
        target_account_id=target_account_id,
        internal_account_id=internal_account_id,
        amount=amount,
        denomination=denomination,
        advice=advice,
    )
    return create_posting_instruction_batch_event(
        instructions=[outbound_hard_settlement_instruction],
        value_datetime=value_datetime,
        instruction_details=instruction_details,
        client_transaction_id=client_transaction_id,
        pib_timestamp=pib_timestamp,
        batch_details=batch_details,
        client_batch_id=client_batch_id,
    )


def create_release_event(
    client_transaction_id: str,
    value_datetime: datetime,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    client_batch_id: Optional[str] = None,
    pib_timestamp: Optional[datetime] = None,
) -> SimulationEvent:
    """
    Returns a SimulationEvent containing a Posting Instruction Batch
    instructing a Release
    :param client_transaction_id: the ID of the client transaction this posting
                                  instruction is creating or mutating.
    :param value_datetime the datetime at which the Posting Instruction Batch
        will be applied in the simulation
    :param instruction_details: An optional mapping containing instruction-level metadata.
    :param batch_details: A dictionary containing batch level metadata.
    :param client_batch_id: An id which allows related PostingInstructions
                            to be associated with each other.
    :param pib_timestamp: Optional value timestamp at which the Posting Instruction Batch
                          will be applied in the simulation. If "None" defaults to value_datetime.
    :return SimulationEvent with a Release Posting Instruction Batch
    """
    release_instruction = Release()
    return create_posting_instruction_batch_event(
        instructions=[release_instruction],
        value_datetime=value_datetime,
        pib_timestamp=pib_timestamp,
        client_transaction_id=client_transaction_id,
        instruction_details=instruction_details,
        batch_details=batch_details,
        client_batch_id=client_batch_id,
    )


def create_settlement_event(
    amount: str,
    client_transaction_id: str,
    value_datetime: datetime,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    client_batch_id: Optional[str] = None,
    final: bool = False,
    pib_timestamp: Optional[datetime] = None,
) -> SimulationEvent:
    """
    Returns a SimulationEvent containing a Posting Instruction Batch
    instructing a Settlement
    :param amount: string representation of the amount to be sent.
    :param client_transaction_id: the ID of the client transaction this posting
                                  instruction is creating or mutating.
    :param value_datetime: the datetime at which the Posting Instruction Batch
                           will be applied in the simulation.
    :param instruction_details: An optional mapping containing instruction-level metadata.
    :param batch_details: A dictionary containing batch level metadata.
    :param client_batch_id: An id which allows related PostingInstructions
                            to be associated with each other.
    :param final: A boolean allowing further (True) or no more (False) settlements associated with
                 the client_transaction_id.
    :param pib_timestamp: Optional value timestamp at which the Posting Instruction Batch
                          will be applied in the simulation. If "None" defaults to value_datetime.
    :return SimulationEvent with a Settlement Posting Instruction Batch
    """
    settlement_instruction = Settlement(amount=amount, final=final)
    return create_posting_instruction_batch_event(
        instructions=[settlement_instruction],
        value_datetime=value_datetime,
        client_transaction_id=client_transaction_id,
        instruction_details=instruction_details,
        pib_timestamp=pib_timestamp,
        batch_details=batch_details,
        client_batch_id=client_batch_id,
    )


def create_transfer_event(
    amount: str,
    value_datetime: datetime,
    creditor_target_account_id: str,
    debtor_target_account_id: str,
    denomination: Optional[str] = None,
    client_transaction_id: Optional[str] = None,
    instruction_details: Optional[Dict[str, str]] = None,
    batch_details: Optional[Dict[str, str]] = None,
    client_batch_id: Optional[str] = None,
    pib_timestamp: Optional[datetime] = None,
) -> SimulationEvent:
    """
    Returns a SimulationEvent containing a Posting Instruction Batch
    instructing a Transfer
    :param amount: string representation of the amount to be sent.
    :param value_datetime: the datetime at which the event will be applied in the simulation.
    :param creditor_target_account_id: account to credit for Transfer.
    :param debtor_target_account_id: account to debit for Transfer.
    :param denomination: the denomination the posting instruction will be in.
    :param client_transaction_id: the ID of the client transaction this posting
                                  instruction is creating or mutating.
    :param instruction_details: An optional mapping containing instruction-level metadata.
    :param batch_details: A dictionary containing batch level metadata.
    :param client_batch_id: An id which allows related PostingInstructions
                            to be associated with each other.
    :param pib_timestamp: Optional value timestamp at which the Posting Instruction Batch
                          will be applied in the simulation. If "None" defaults to value_datetime.
    :return: SimulationEvent with a Transfer Posting Instruction Batch
    """
    transfer_instruction = Transfer(
        creditor_target_account_id=creditor_target_account_id,
        debtor_target_account_id=debtor_target_account_id,
        amount=amount,
        denomination=denomination,
    )
    return create_posting_instruction_batch_event(
        instructions=[transfer_instruction],
        value_datetime=value_datetime,
        client_transaction_id=client_transaction_id,
        batch_details=batch_details,
        instruction_details=instruction_details,
        client_batch_id=client_batch_id,
        pib_timestamp=pib_timestamp,
    )


def create_posting_instruction_batch_event(
    instructions: List[Instruction],
    value_datetime: datetime,
    client_transaction_id: str = None,
    override: bool = False,
    batch_details: str = None,
    instruction_details: str = None,
    client_batch_id: str = None,
    client_id: str = None,
    pib_timestamp: Optional[datetime] = None,
) -> SimulationEvent:
    return SimulationEvent(
        value_datetime,
        create_posting_instruction_batch(
            instructions=instructions,
            value_datetime=datetime.isoformat(pib_timestamp or value_datetime),
            client_transaction_id=client_transaction_id,
            override=override,
            batch_details=batch_details,
            instruction_details=instruction_details,
            client_batch_id=client_batch_id,
            client_id=client_id,
        ),
    )
