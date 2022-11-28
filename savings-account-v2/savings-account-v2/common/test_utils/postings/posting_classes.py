# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
import uuid
from common.test_utils.postings.override_classes import Override


DEFAULT_INTERNAL_ACCOUNT = "1"
DEFAULT_TARGET_ACCOUNT = "Main account"
DEFAULT_CLIENT_ID = "AsyncCreatePostingInstructionBatch"
DEFAULT_CLIENT_BATCH_ID = str(uuid.uuid4())
DEFAULT_CLIENT_TRANSACTION_ID = str(uuid.uuid4())
DEFAULT_BATCH_DETAILS = {}
DEFAULT_INSTRUCTION_DETAILS = {}
DEFAULT_DENOMINATION = "GBP"
DEFAULT_ADVICE = False
DEFAULT_FINAL = False
DEFAULT_ASSET = "COMMERCIAL_BANK_MONEY"


class PostingInstructionBatchEvent:
    """
    This class represents a Posting Instruction Batch event that can be consumed
    by the Simulation endpoint to instruct the movement of money.
    Please note that `.to_dict()` must be called when the object is passed into vault_caller.

    Args:
        param posting_instructions: a list of PostingInstruction objects.
        param value_timestamp: a isoformat string of the datetime
            the Posting Instruction Batch should occur at.
        param client_batch_id: an id that allows to collerate multiple
            Posting Instruction Batches together.
        param client_id: id of the client Vault should use to fulfill the postings.
        param batch_details: free from dict containing extra information about the PIB.
    """

    def __init__(
        self,
        posting_instructions,
        value_timestamp,
        client_batch_id=DEFAULT_CLIENT_BATCH_ID,
        client_id=None,
        batch_details=None,
    ):
        self.batch_details = batch_details or DEFAULT_BATCH_DETAILS
        self.client_id = client_id or DEFAULT_CLIENT_ID
        self.client_batch_id = client_batch_id
        self.value_timestamp = value_timestamp
        self.posting_instructions = posting_instructions

    def to_dict(self):
        return {
            "posting_instruction_batch": {
                "client_id": self.client_id,
                "client_batch_id": self.client_batch_id,
                "posting_instructions": [
                    post.to_dict() for post in self.posting_instructions
                ],
                "batch_details": self.batch_details,
                "value_timestamp": self.value_timestamp,
            }
        }


class PostingInstruction:
    """
    This class represents an individual posting instruction object.

    Args:
        param instruction: the instruction object, must be a supported posting instruction type.
        param client_transaction_id: id to identify and collerate this posting instruction.
        param instruction_details: dict of free form information about the posting instruction.
        param override: object that defines which restrictions should be overriten if any.
    """

    def __init__(
        self,
        instruction,
        client_transaction_id=None,
        instruction_details=None,
        override=False,
    ):
        if override:
            override = Override(override_all=True)
        else:
            override = Override()
        self.client_transaction_id = client_transaction_id or str(uuid.uuid4())
        self.instruction_details = instruction_details or DEFAULT_INSTRUCTION_DETAILS
        self.override = override
        self.instruction = instruction

    def to_dict(self):
        return {
            "client_transaction_id": self.client_transaction_id,
            "instruction_details": self.instruction.instruction_details
            if hasattr(self.instruction, "instruction_details")
            and self.instruction.instruction_details
            else self.instruction_details,
            "override": self.override.to_dict(),
            **self.instruction.to_dict(),
        }


class Instruction:
    def __init__(self, amount, denomination, type):
        self.amount = amount
        self.denomination = denomination or DEFAULT_DENOMINATION
        self.type = type


class OutboundAuthorisation(Instruction):
    """
    This class represents an Outbound Authorisation Posting Instruction type

    Args:
        param amount: string represenation of the amount to be sent
        param target_account_id: account from which the money should be authorised
        param denomination: denomination in which the money should be sent
        param internal_account_id: account to which the money should be sent
        param advice: if true, the amount will be authorised regardless of balance checks
        param payment_device_token: payment_device_token to which the money should be sent
    """

    def __init__(
        self,
        amount,
        target_account_id=DEFAULT_TARGET_ACCOUNT,
        denomination=DEFAULT_DENOMINATION,
        internal_account_id=DEFAULT_INTERNAL_ACCOUNT,
        advice=DEFAULT_ADVICE,
        payment_device_token=None,
    ):
        super().__init__(amount, denomination, "outbound_authorisation")
        self.payment_device_token = payment_device_token
        self.target_account_id = target_account_id or DEFAULT_TARGET_ACCOUNT
        self.internal_account_id = internal_account_id or DEFAULT_INTERNAL_ACCOUNT
        self.advice = advice
        if payment_device_token:
            self.target_account = {"payment_device_token": self.payment_device_token}
        else:
            self.target_account = {"account_id": self.target_account_id}

    def to_dict(self):
        return {
            self.type: {
                "amount": self.amount,
                "denomination": self.denomination,
                "target_account": self.target_account,
                "internal_account_id": self.internal_account_id,
                "advice": self.advice,
            }
        }


class InboundAuthorisation(Instruction):
    """
    This class represents an Inbound Authorisation Posting Instruction type

    Args:
        param amount: string represenation of the amount to be sent
        param target_account_id: account to which the money should be sent
        param denomination: denomination in which the money should be sent
        param internal_account_id: account from which the money should be authorised
        param advice: if true, the amount will be authorised regardless of balance check
        param payment_device_token: payment_device_token to which the money should be sent
    """

    def __init__(
        self,
        amount,
        target_account_id=DEFAULT_TARGET_ACCOUNT,
        denomination=DEFAULT_DENOMINATION,
        internal_account_id=DEFAULT_INTERNAL_ACCOUNT,
        advice=DEFAULT_ADVICE,
        payment_device_token=None,
    ):
        super().__init__(amount, denomination, "inbound_authorisation")
        self.payment_device_token = payment_device_token
        self.target_account_id = target_account_id or DEFAULT_TARGET_ACCOUNT
        self.internal_account_id = internal_account_id or DEFAULT_INTERNAL_ACCOUNT
        self.advice = advice
        if payment_device_token:
            self.target_account = {"payment_device_token": self.payment_device_token}
        else:
            self.target_account = {"account_id": self.target_account_id}

    def to_dict(self):
        return {
            self.type: {
                "amount": self.amount,
                "denomination": self.denomination,
                "target_account": self.target_account,
                "internal_account_id": self.internal_account_id,
                "advice": self.advice,
            }
        }


class AuthorisationAdjustment(Instruction):
    """
    This class represents an Authorisation Adjustment Instruction type

    Args:
        param amount: string represenation of the amount to be adjusted
        param advice: if true, the amount will be authorised regardless of balance check
    """

    def __init__(self, amount, advice=DEFAULT_ADVICE):
        super().__init__(amount, "", "authorisation_adjustment")
        self.advice = advice

    def to_dict(self):
        return {self.type: {"amount": self.amount, "advice": self.advice}}


class Settlement(Instruction):
    """
    This class represents a Settlement Instruction type

    Args:
        param amount: string represenation of the amount to be sent
        param final: if true, the amount will be overwritten by the full
            authorisation amount
    """

    def __init__(self, amount, final=DEFAULT_FINAL):
        super().__init__(amount, "", "settlement")
        self.final = final

    def to_dict(self):
        return {self.type: {"amount": self.amount, "final": self.final}}


class Release(Instruction):
    """
    This class represents a Release Instruction type
    """

    def __init__(
        self,
    ):
        super().__init__("", "", "release")

    def to_dict(self):
        return {self.type: {}}


class InboundHardSettlement(Instruction):
    """
    This class represents an Inbound Hard Settlement Posting Instruction type

    Args:
        param amount: string represenation of the amount to be sent
        param target_account_id: account to which the money should be sent
        param denomination: denomination in which the money should be sent
        param internal_account_id: account from which the money should be authorised
        param advice: if true, the amount will be authorised regardless of balance check
        param payment_device_token: payment_device_token to which the money should be sent
    """

    def __init__(
        self,
        amount,
        target_account_id=DEFAULT_TARGET_ACCOUNT,
        denomination=DEFAULT_DENOMINATION,
        internal_account_id=DEFAULT_INTERNAL_ACCOUNT,
        advice=DEFAULT_ADVICE,
        payment_device_token=None,
    ):
        super().__init__(amount, denomination, "inbound_hard_settlement")
        self.payment_device_token = payment_device_token
        self.target_account_id = target_account_id or DEFAULT_TARGET_ACCOUNT
        self.internal_account_id = internal_account_id or DEFAULT_INTERNAL_ACCOUNT
        self.advice = advice
        if payment_device_token:
            self.target_account = {"payment_device_token": self.payment_device_token}
        else:
            self.target_account = {"account_id": self.target_account_id}

    def to_dict(self):
        return {
            self.type: {
                "amount": self.amount,
                "denomination": self.denomination,
                "target_account": self.target_account,
                "internal_account_id": self.internal_account_id,
                "advice": self.advice,
            }
        }


class OutboundHardSettlement(Instruction):
    """
    This class represents an Outbound Hard Settlement Posting Instruction type

    Args:
        param amount: string represenation of the amount to be sent
        param target_account_id: account to which the money should be sent
        param denomination: denomination in which the money should be sent
        param internal_account_id: account from which the money should be authorised
        param advice: if true, the amount will be authorised regardless of balance check
        param payment_device_token: payment_device_token to which the money should be sent
    """

    def __init__(
        self,
        amount,
        target_account_id=DEFAULT_TARGET_ACCOUNT,
        denomination=DEFAULT_DENOMINATION,
        internal_account_id=DEFAULT_INTERNAL_ACCOUNT,
        advice=DEFAULT_ADVICE,
        payment_device_token=None,
        instruction_details=None,
    ):
        super().__init__(amount, denomination, "outbound_hard_settlement")
        self.payment_device_token = payment_device_token
        self.target_account_id = target_account_id or DEFAULT_TARGET_ACCOUNT
        self.internal_account_id = internal_account_id or DEFAULT_INTERNAL_ACCOUNT
        self.advice = advice
        if payment_device_token:
            self.target_account = {"payment_device_token": self.payment_device_token}
        else:
            self.target_account = {"account_id": self.target_account_id}
        self.instruction_details = instruction_details

    def to_dict(self):
        return {
            self.type: {
                "amount": self.amount,
                "denomination": self.denomination,
                "target_account": self.target_account,
                "internal_account_id": self.internal_account_id,
                "advice": self.advice,
                "instruction_details": self.instruction_details,
            }
        }


class Transfer(Instruction):
    """
    This class represents a Transfer Posting Instruction type

    Args:
        param amount: string represenation of the amount to be sent
        param debtor_target_account: account from which the money should be sent
        param denomination: denomination in which the money should be sent
        param creditor_target_account: account to which the money should be sent
    """

    def __init__(
        self,
        amount,
        debtor_target_account_id,
        creditor_target_account_id,
        denomination=DEFAULT_DENOMINATION,
        instruction_details=None,
    ):
        super().__init__(amount, denomination, "transfer")
        self.debtor_target_account_id = debtor_target_account_id
        self.creditor_target_account_id = creditor_target_account_id
        self.instruction_details = instruction_details

    def to_dict(self):
        return {
            self.type: {
                "amount": self.amount,
                "denomination": self.denomination,
                "debtor_target_account": {"account_id": self.debtor_target_account_id},
                "creditor_target_account": {
                    "account_id": self.creditor_target_account_id
                },
                "instruction_details": self.instruction_details,
            }
        }


class CustomInstruction(Instruction):
    def __init__(self, postings):

        super().__init__("", "", "custom_instruction")
        self.postings = postings

    def to_dict(self):
        return {self.type: {"postings": [post.to_dict() for post in self.postings]}}


class Posting:
    def __init__(
        self,
        account_id,
        amount,
        credit,
        denomination=None,
        asset=None,
        account_address=None,
        phase=None,
    ):

        self.account_id = account_id
        self.amount = amount
        self.denomination = denomination or DEFAULT_DENOMINATION
        self.asset = asset or DEFAULT_ASSET
        self.account_address = account_address or "DEFAULT"
        self.phase = phase or "POSTING_PHASE_COMMITTED"
        self.credit = credit

    def to_dict(self):
        return {
            "account_id": self.account_id,
            "amount": self.amount,
            "denomination": self.denomination,
            "asset": self.asset,
            "account_address": self.account_address,
            "phase": self.phase,
            "credit": self.credit,
        }
