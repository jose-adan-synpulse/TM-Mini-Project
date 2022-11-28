# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
import builtins
from types import FunctionType
from unittest.mock import Mock
from inspect import isfunction

from .types_extension import _ALL_TYPES, _WHITELISTED_BUILTINS, _SUPPORTED_HOOK_NAMES


def _mock_requires_decorator(
    parameters=None,
    balances=None,
    flags=None,
    postings=None,
    last_execution_time=None,
    event_type=None,
    calendar=None,
    modules=None,
):
    def inner(func):
        return func

    return inner


def run(
    smart_contract_code: str, function_name: str, vault_object: Mock, *args, **kwargs
):
    """Runs function `function_name` that is defined in the `smart_contract_code`.

    The function will run in a similar environment as when executed in Vault:
    - Only some builtins are available (see Vault Smart Contract documentation for full list).
    - The passed-in `vault` object will be available as a global variable to the function being run.
      This will only happen for hooks (i.e. a helper function wouldn't have access to it).
    - Types (see Vault Smart Contract documentation for full list) are globally available.

    Args:
        smart_contract_code: The source code of the Smart Contract.
        function_name: The name of the function to run, this must be defined in the Smart Contract
            code. It can be either a Vault Smart Contract hook, or any other defined function.
        vault_object: The mock Vault object to make available to the function being run. Will only
            be available for hook functions.
        *args: Additional arguments to call `function_name` with.
        **kwargs: Additional named arguments to call `function_name` with.
    """
    # The function we will execute will only have access to symbols defined in the sandbox and the
    # symbols defined by the Smart Contract itself.
    sandbox = {
        "__builtins__": {
            name: getattr(builtins, name) for name in _WHITELISTED_BUILTINS
        },
        "requires": _mock_requires_decorator,
        **_ALL_TYPES,
    }

    exec(smart_contract_code, sandbox, sandbox)

    func = sandbox.get(function_name)
    if func is None:
        raise ValueError(
            f'Function "{function_name}" does not exist in provided Smart Contract code'
        )

    # Make sure `vault` is only accessible to `function_name` if it is a known hook, and not any
    # function it calls or non-hook functions, unless the function is a contract module. That is
    # why we don't put it directly in the sandbox and do this checking here.
    function_globals = {**sandbox}
    if function_name in _SUPPORTED_HOOK_NAMES or kwargs.get("contract_module", False):
        function_globals["vault"] = vault_object
        kwargs.pop("contract_module", None)

    func = FunctionType(
        func.__code__,
        function_globals,
        func.__name__,
        func.__defaults__,
        func.__closure__,
    )

    return func(*args, **kwargs)


class ContractModuleRunner:
    """
    An instance of the class binds functions from a contract module's code to itself.
    The functions will run in a similar environment as when executed in Vault:
    - Only some builtins are available (see Vault Smart Contract documentation for full list).
      This will only happen for hooks (i.e. a helper function wouldn't have access to it).
    - Types (see Vault Smart Contract documentation for full list) are globally available.
    """

    def __init__(self, module_code: str) -> None:
        # The functions will only have access to symbols defined in the sandbox
        # and the symbols defined by the Smart Contract itself.
        sandbox = {
            "__builtins__": {
                name: getattr(builtins, name) for name in _WHITELISTED_BUILTINS
            },
            "requires": _mock_requires_decorator,
            # TODO(Contracts SDK) - Use distinct set of types supported in Contract Modules
            **_ALL_TYPES,
        }
        sandbox_built_ins = sandbox.copy()

        exec(module_code, sandbox, sandbox)

        for attr_name, attr in sandbox.items():
            # We only want the functions defined in the Contract Module
            if attr_name in sandbox_built_ins or not isfunction(attr):
                continue
            func, function_name = attr, attr_name
            func = FunctionType(
                func.__code__,
                {**sandbox},
                func.__name__,
                func.__defaults__,
                func.__closure__,
            )
            # Set each function as an attribute of the ContractModuleRunner instance
            setattr(self, function_name, func)
