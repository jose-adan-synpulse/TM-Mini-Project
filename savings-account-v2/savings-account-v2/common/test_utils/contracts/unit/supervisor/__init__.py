# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
import builtins
from types import FunctionType
from unittest.mock import Mock
from .types_extension import _ALL_TYPES, _WHITELISTED_BUILTINS, _SUPPORTED_HOOK_NAMES


def _mock_requires_decorator(
    parameters=None,
    balances=None,
    flags=None,
    postings=None,
    last_execution_time=None,
    event_type=None,
    data_scope=None,
    supervisee_hook_directives=None,
):
    def inner(func):
        return func

    return inner


def run(
    supervisor_contract_code: str,
    function_name: str,
    vault_object: Mock,
    *args,
    **kwargs,
):
    """Runs function `function_name` that is defined in the `supervisor_contract_code`.

    The function will run in a similar environment as when executed in Vault:
    - Only some builtins are available (see Vault Supervisor Contract documentation for full list).
    - The passed-in `vault` object will be available as a global variable to the function being run.
      This will only happen for hooks (i.e. a helper function wouldn't have access to it).
    - Types (see Vault Supervisor Contract documentation for full list) are globally available.

    Args:
        supervisor_contract_code: The source code of the Supervisor Contract.
        function_name: The name of the function to run, this must be defined in Supervisor Contract
            code. It can be either a Vault Supervisor Contract hook, or any other defined function.
        vault_object: The mock Vault object to make available to the function being run. Will only
            be available for hook functions.
        *args: Additional arguments to call `function_name` with.
        **kwargs: Additional named arguments to call `function_name` with.
    """
    # The function we will execute will only have access to symbols defined in the sandbox and the
    # symbols defined by the Supervisor Contract itself.
    sandbox = {
        "__builtins__": {
            name: getattr(builtins, name) for name in _WHITELISTED_BUILTINS
        },
        "requires": _mock_requires_decorator,
        **_ALL_TYPES,
    }

    exec(supervisor_contract_code, sandbox, sandbox)

    func = sandbox.get(function_name)
    if func is None:
        raise ValueError(
            f'Function "{function_name}" does not exist in provided Supervisor Contract code'
        )

    # Make sure `vault` is only accessible to `function_name` if it is a known hook, and not any
    # function it calls or non-hook functions. That is why we don't put it directly in the sandbox
    # and do this checking here.
    function_globals = {**sandbox}
    if function_name in _SUPPORTED_HOOK_NAMES:
        function_globals["vault"] = vault_object

    func = FunctionType(
        func.__code__,
        function_globals,
        func.__name__,
        func.__defaults__,
        func.__closure__,
    )

    return func(*args, **kwargs)
