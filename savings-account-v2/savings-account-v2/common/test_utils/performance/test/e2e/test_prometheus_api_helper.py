# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# standard libs
import time
from datetime import datetime, timezone
from requests import HTTPError

# third party
from dateutil.relativedelta import relativedelta

# common
import common.test_utils.endtoend as endtoend
import common.test_utils.performance.prometheus_api_helper as prometheus
from common.test_utils.performance.test_types import PerformanceTestType


class PrometheusApiHelperTest(endtoend.End2Endtest):
    def setUp(self):
        self._started_at = time.time()

    def tearDown(self):
        self._elapsed_time = time.time() - self._started_at
        # Uncomment this for timing info.
        # print('\n{} ({}s)'.format(self.id().rpartition('.')[2], round(self._elapsed_time, 2)))

    def test_evaluate_query(self):
        query = prometheus.Query(
            name="sample-query",
            kubernetes_namespace="non-existing-namespace",
            kubernetes_cluster="non-existing-cluster",
            app="contract-engine",
            query='up{kubernetes_namespace=~"$kubernetes_namespace",\n  '
            'kubernetes_cluster="$cluster",\n  app="$app"}',
        )
        result = prometheus.evaluate_query(
            query.format_query(), datetime.now(tz=timezone.utc)
        )
        self.assertEquals(result["result"], [])
        self.assertEquals(result["resultType"], "vector")

    def test_evaluate_query_range(self):
        query = prometheus.Query(
            name="sample-query",
            kubernetes_namespace="non-existing-namespace",
            kubernetes_cluster="non-existing-cluster",
            app="contract-engine",
            query='up{kubernetes_namespace=~"$kubernetes_namespace",\n  '
            'kubernetes_cluster="$cluster",\n  app="$app"}',
        )
        result = prometheus.evaluate_query_range(
            query.format_query(),
            datetime.now(tz=timezone.utc) - relativedelta(minutes=1),
            datetime.now(tz=timezone.utc),
        )
        self.assertEquals(result["result"], [])
        self.assertEquals(result["resultType"], "matrix")

    def test_end_before_start_raises_error(self):
        with self.assertRaises(HTTPError):
            prometheus.get_results(
                start=datetime.now(tz=timezone.utc),
                end=datetime.now(tz=timezone.utc) - relativedelta(hours=1),
            )

    def test_get_queries_returns_query_object_list(self):
        queries_list = prometheus.get_queries(
            app="test-sample-app",
            cluster="test-cluster",
            namespace="test-namespace",
            lookback_window="2",
            test_type=PerformanceTestType.SCHEDULES,
        )

        for query in queries_list:
            self.assertIsInstance(query, prometheus.Query)


if __name__ == "__main__":
    endtoend.runtests()
