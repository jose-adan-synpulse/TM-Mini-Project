# Copyright @ 2021 Thought Machine Group Limited. All rights reserved.
# standard libs
import argparse
import csv
import json
import sys
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple, Union
from statistics import mean
from io import StringIO
from zipfile import ZipFile, ZIP_DEFLATED

# third party
from dateutil.relativedelta import relativedelta

# common
import common.test_utils.endtoend as endtoend
from common.python.resources import resource_string
from common.test_utils.performance.test_types import PerformanceTestType

PERFORMANCE_QUERIES_FILE = "common/test_utils/performance/performance_queries.json"
CONFIG_FILE = "common/test_utils/endtoend/config.json"
PERCENTILE_95TH = "95th %ile"
PERCENTILE_99TH = "99th %ile"

DEFAULT_APP = "contract-engine"
DEFAULT_ENV = "skyfall_inception"
DEFAULT_OUTPUT_PATH = ""


@dataclass
class Query:
    name: str
    query: str
    app: str = None
    kubernetes_cluster: str = None
    kubernetes_namespace: str = None
    lookback_window: int = None
    param_var: str = None
    param: str = None

    def format_query(self):
        query = self.query[:]

        if self.kubernetes_cluster:
            query = query.replace("$cluster", self.kubernetes_cluster)
        if self.kubernetes_namespace:
            # sometimes specified as namespace or kubernetes_namespace
            query = query.replace("$namespace", self.kubernetes_namespace)
            query = query.replace("$kubernetes_namespace", self.kubernetes_namespace)
        if self.lookback_window:
            query = query.replace("$lookback_window", str(self.lookback_window) + "m")
        if self.app:
            query = query.replace("$app", self.app)
        if self.param_var:
            query = query.replace(self.param_var, str(self.param))

        # possible outcome values ".*" for all, "Success", "Error", "PartialFailure", "Filtered"
        query = query.replace("$outcome", "Success")

        return query


def process_args(args: list) -> Tuple[argparse.Namespace, List]:
    """
    Process command-line arguments to the tool
    """

    parser = argparse.ArgumentParser(
        description="Prometheus API helper generates a CSV file "
        "with results from queries stored in performance_queries.json. "
        "Optional arguments are available to customise result output of "
        "the query - percentile (default: 99th percentile), "
        "lookback window (default: 2 minutes) and query range step (default: 30 seconds)."
    )
    schedules_type = PerformanceTestType.SCHEDULES.value
    postings_type = PerformanceTestType.POSTINGS.value

    # Common arguments
    parser.add_argument(
        "--start",
        required=True,
        default=datetime.now(tz=timezone.utc),
        action="store",
        help="Start timestamp for capturing performance results - 'YYYY-MM-DD HH:MM:SS'",
    )
    parser.add_argument(
        "--end",
        required=True,
        default=datetime.now(tz=timezone.utc),
        action="store",
        help="End timestamp for capturing performance results - 'YYYY-MM-DD HH:MM:SS'",
    )
    parser.add_argument(
        "--test_type",
        required=False,
        default=schedules_type,
        type=str,
        action="store",
        help=f"Type of performance test, either '{schedules_type}' "
        f"or '{postings_type}' [Default: '{schedules_type}']",
    )
    parser.add_argument(
        "--apps",
        nargs="+",
        required=False,
        default=[DEFAULT_APP],
        action="store",
        help=f"The apps for which data is extracted [Default: {[DEFAULT_APP]}] "
        f"Use space separated values, e.g. --apps app-1 app-2",
    )
    parser.add_argument(
        "--percentile",
        required=False,
        default=PERCENTILE_99TH,
        action="store",
        help="Results percentile for queries with percentiles [Default: 99th %%ile]",
    )
    parser.add_argument(
        "--lookback_window",
        required=False,
        default="2",
        action="store",
        help="Lookback window (in minutes) [Default: 2]",
    )
    parser.add_argument(
        "--query_range_step",
        required=False,
        default="30",
        action="store",
        help="Query range step for result output (in seconds) [Default: 30]",
    )
    parser.add_argument(
        "--environment",
        required=False,
        default=DEFAULT_ENV,
        action="store",
        help=f"Environment for which to run queries [Default: {DEFAULT_ENV}]",
    )
    parser.add_argument(
        "--output_path",
        required=False,
        default=DEFAULT_OUTPUT_PATH,
        action="store",
        help="Output path for results files",
    )

    known_args, unknown_args = parser.parse_known_args(args)

    # transform start and end to datetime
    known_args.start = datetime.strptime(known_args.start, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )
    known_args.end = datetime.strptime(known_args.end, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )
    # parse string value back to enum
    known_args.test_type = PerformanceTestType(known_args.test_type)

    return known_args, unknown_args


def get_results(
    start: str,
    end: str,
    apps: List[str] = None,
    percentile: str = PERCENTILE_99TH,
    lookback_window: int = 2,
    query_range_step: int = 30,
    environment: str = DEFAULT_ENV,
    output_path: str = DEFAULT_OUTPUT_PATH,
    test_type: str = PerformanceTestType.SCHEDULES,
) -> None:
    """
    Writes a file with results from queries that are configured in the performance_queries.json
    file in performance test utils folder.
    Result CSV files can be found in a zip file in $plz-out/$test_path/...

    :param start: performance test start time
    :param end: performance test end time
    :param apps: names of the apps to query for
    :param percentile: results percentile
    :param lookback_window: lookback window in minutes
    :param query_range_step: query range step for result output in seconds
    :param enviroment: environment name from env config file
    :param output_path: output path for results files
    :param test_type: test type defined at root of query file (postings or schedules)

    :return None
    """

    if not apps:
        apps = [DEFAULT_APP]

    endtoend.testhandle.environment = environment
    endtoend.helper.setup_environments()

    env_config = endtoend.testhandle.available_environments[
        endtoend.testhandle.environment
    ]
    cluster = env_config.cluster
    namespace = env_config.namespace

    results_by_app = {}
    for app in apps:
        result_list = {}
        all_queries = get_queries(
            test_type,
            app,
            cluster,
            namespace,
            lookback_window,
            percentile,
        )

        for query in all_queries:
            result = evaluate_query_range(
                query=query.format_query(),
                start=start,
                end=end,
                step=query_range_step,
            )

            result_list[query.name] = parse_result(result)

        results = transpose_results(result_list)
        results_by_app[app] = split(results)

    create_reports(output_path, results_by_app, test_type)


def get_queries(
    test_type: PerformanceTestType,
    app: str,
    cluster: str,
    namespace: str,
    lookback_window: str,
    percentile=PERCENTILE_99TH,
) -> List[Query]:
    """
    Returns all queries from config file with parameters replaced with variables.
    For percentile queries additional config is processed where queries are multiplied
    by the number of params available.
    TODO: INC-4230 get the queries from Grafana APIs, and store path of query in config files.

    :param test_type: type of the test which is run (postings or schedules)
    :param app: name of the app to query for
    :param cluster: target environment kubernetes cluster
    :param namespace: target environment kubernetes namespace
    :param lookback_window: lookback window in minutes
    :param percentile: results percentile

    :return: lists of queries
    """
    queries_list = []
    queries_file_contents = resource_string(PERFORMANCE_QUERIES_FILE)
    queries_json = json.loads(queries_file_contents)
    test_queries = queries_json.get(test_type.value, [])

    for query_name, query_value in test_queries.items():
        if query_value.get(percentile):
            param_var = query_value.get("param_variable")
            query_params = query_value.get("params")
            # TODO improve query handling when there is no query param
            for param in query_params or [None]:
                display_name = f"{query_name}.{param}" if param else query_name
                query = Query(
                    name=f"{display_name} ({percentile})",
                    app=app,
                    kubernetes_cluster=cluster,
                    kubernetes_namespace=namespace,
                    lookback_window=lookback_window,
                    query=query_value.get(percentile),
                    param_var=param_var,
                    param=param,
                )
                queries_list.append(query)
        else:
            query = Query(
                name=query_name,
                app=app,
                kubernetes_cluster=cluster,
                kubernetes_namespace=namespace,
                lookback_window=lookback_window,
                query=query_value.get("query"),
                param_var=None,
                param=None,
            )
            queries_list.append(query)
    return queries_list


def evaluate_query(
    query: str,
    time: datetime,
) -> Dict[str, Dict[str, Any]]:
    """
    Evaluates an instant query at single point of time.
    :param query: prometheus query language query string
    :param time: time at which query needs to be executed
    :return: all requested data
    """
    params = {"query": query, "time": _datetime_to_rfc_3339(time)}
    return endtoend.helper.send_request("get", "/api/v1/query", params=params)["data"]


def evaluate_query_range(
    query: str,
    start: datetime,
    end: datetime,
    step: int = 30,
) -> Dict[str, Dict[str, Any]]:
    """
    Evaluates an instant query between specified start and end date.
    :param query: prometheus query language query string
    :param start: start time of the resulting data
    :param end: end time of the resulting data
    :param step: step in seconds, defaulted to 30s
    :return: all requested data
    """
    params = {
        "query": query,
        "start": _datetime_to_rfc_3339(start),
        "end": _datetime_to_rfc_3339(end),
        "step": step,
    }
    return endtoend.helper.send_request("get", "/api/v1/query_range", params=params)[
        "data"
    ]


def parse_result(result: Dict[str, Dict[str, Any]]) -> List[Tuple[datetime, Decimal]]:
    """
    Parses result for resultType "matrix" creating dict of timestamps and converting all
    number values to Decimal. Values of "NaN" are converted to None.

    :param result: result from query api (query or query_range)

    :return: formatted response with datetime and decimal values
    """

    def value_to_decimal(value):
        return None if value == "NaN" else Decimal(value)

    if result["result"] and result["resultType"] == "matrix":
        result_data = result["result"][0]["values"]
        return [
            (
                datetime.fromtimestamp(date).astimezone(timezone.utc),
                value_to_decimal(value),
            )
            for date, value in result_data
        ]
    else:
        return []


def transpose_results(result_list: List[Tuple[datetime, Decimal]]) -> List[List[str]]:
    """
    Converts rows to columns and adds a column name for Datetime.
    Since query range returns full result set, assumption is that full data set for
    the date range is available.

    :param result_list: results from query_range call

    :return: List of lists of a result table
    """
    transposed_results = []
    for query_name, col in result_list.items():
        if col:
            current_date_col, value_col = zip(*col)
            if not transposed_results:
                transposed_results.append(["Datetime"] + list(current_date_col))
            transposed_results.append([query_name] + list(value_col))

    return list(map(list, zip(*transposed_results)))


def split(
    results_list: List[List[Union[datetime, Decimal, None]]]
) -> List[List[Union[datetime, Decimal]]]:
    """
    Splits result set based on rows where values are only either Decimal("0") or None.
    Produces a list of lists with valid values.

    :param results_list: list of transposed results containing date and values for each resource

    :returns: list of lists of transposed results
    """
    grouped_results_list = []
    group_list = []
    header = []

    split_values = [Decimal("0"), None]
    header = results_list[0]
    for values in results_list[1:]:
        if any(x not in split_values for x in values[1:]):
            group_list.append(values)
        else:
            if group_list:
                grouped_results_list.append([header] + group_list)
            group_list = []

    if group_list:
        grouped_results_list.append([header] + group_list)

    return grouped_results_list


def create_reports(
    output_path: str,
    results_by_app: Dict[str, List],
    test_type: PerformanceTestType,
):
    """
    Writes results list into separate CSV files.

    :param output_path: output path for results files, defaults to root repo
    :param results_by_app: map of app name to list of split result sets
    :param test_type: Enum, the type of performance test
    """
    report_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    abs_file_path = os.path.join(os.getcwd(), output_path)
    # Files in plz-out/tmp get cleaned up, so move out of this directory
    abs_file_path = abs_file_path.replace("plz-out/tmp/", "plz-out/")
    os.makedirs(os.path.dirname(abs_file_path), exist_ok=True)
    zip_file = ZipFile(
        f"{abs_file_path}perf-results-D{report_date}-{test_type.value}.zip",
        "w",
        ZIP_DEFLATED,
    )
    for app, results_list in results_by_app.items():
        for idx, result_file in enumerate(results_list):
            # StringIO buffer simulates the csv file without needing to create it
            csv_string_buffer = StringIO()
            csv_out = csv.writer(csv_string_buffer)
            csv_out.writerows(result_file)
            # Add the csv to our zip folder
            zip_file.writestr(
                f"{app}-report-{idx}.csv",
                csv_string_buffer.getvalue(),
            )
    zip_file.close()


def average(
    result: List[Tuple[datetime, Decimal]], averaging_window: int = 120
) -> Decimal:
    """
    Retruns average (minus averaging window) from timeseries of values
    :param result: parsed result from the query
    :param averaging_window: window in seconds to ignore from start and end of the result set

    :return: average value of the data set
    """
    data_for_mean = [
        value
        for date, value in result
        if date > result[0][0] + relativedelta(seconds=averaging_window)
        and date < result[-1][0] - relativedelta(seconds=averaging_window)
    ]

    return mean(data_for_mean) if data_for_mean else Decimal(0)


def maximum(result: List[Tuple[datetime, Decimal]]) -> Decimal:
    """
    Returns max value from timeseries of values
    """
    return max(result, key=lambda item: item[1])[1]


def _datetime_to_rfc_3339(dt):
    timezone_aware = dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None

    if not timezone_aware:
        raise ValueError("The datetime object passed in is not timezone-aware")

    return dt.astimezone().isoformat()


if __name__ == "__main__":
    known_args, unknown_args = process_args(sys.argv[1:])
    get_results(**vars(known_args))
