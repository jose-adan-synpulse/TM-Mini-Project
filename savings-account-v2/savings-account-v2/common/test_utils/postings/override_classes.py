# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
DEFAULT_OVERRIDE_ALL = False


class Override:
    """
    This class provides the ability to instruct posting batches
    to ignore any or all restrictions on an account.
    Please note that `.to_dict()` must be called when the object is passed into vault_caller.
    """

    def __init__(self, override_all=DEFAULT_OVERRIDE_ALL, restriction_ids=None):
        self.override_all = override_all
        self.restriction_ids = restriction_ids or []

    def to_dict(self):
        return self.__dict__ if self.override_all or self.restriction_ids else {}
