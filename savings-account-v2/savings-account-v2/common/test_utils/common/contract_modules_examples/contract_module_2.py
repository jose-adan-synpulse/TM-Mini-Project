# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
"""
This is a second contract module example:
"""

api = "3.9.0"
display_name = "dummy module"
description = "dummy module"


def get_parameter_2(
    vault, name, at=None, is_json=False, optional=False, default_value=None
):
    """
    Get the parameter value for a given parameter
    :param vault:
    :param name: string, name of the parameter to retrieve
    :param at: Optional datetime, time at which to retrieve the parameter value. If not
    specified the latest value is retrieved
    :param is_json: Optional boolean, if true json_loads is called on the retrieved parameter value
    :param optional: Optional boolean, if true we treat the parameter as optional
    :param default_value: Optional, if the optional function parameter is True, and the optional
    parameter is not set, this value is returned
    :return:
    """

    if at:
        parameter = vault.get_parameter_timeseries(name=name).at(timestamp=at)
    else:
        parameter = vault.get_parameter_timeseries(name=name).latest()

    if optional:
        parameter = parameter.value if parameter.is_set() else default_value

    if is_json:
        parameter = json_loads(parameter)

    return parameter
