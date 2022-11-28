# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
# standard libs
import time
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

# common
import common.test_utils.performance.prometheus_api_helper as prometheus


class PrometheusApiHelperTest(TestCase):
    def setUp(self):
        self._started_at = time.time()

    def tearDown(self):
        self._elapsed_time = time.time() - self._started_at
        # Uncomment this for timing info.
        # print('\n{} ({}s)'.format(self.id().rpartition('.')[2], round(self._elapsed_time, 2)))

    def test_parse_result(self):
        sample_result = {}
        sample_result["result"] = []
        sample_result["resultType"] = "matrix"
        sample_result["result"].append(
            {"values": [[1628690400, "1.5"], [1628690430, "NaN"], [1628690460, "0"]]}
        )
        parsed_result = prometheus.parse_result(sample_result)

        self.assertTupleEqual(
            parsed_result[0],
            (datetime(2021, 8, 11, 14, 0, 0, tzinfo=timezone.utc), Decimal("1.5")),
        )
        self.assertTupleEqual(
            parsed_result[1],
            (datetime(2021, 8, 11, 14, 0, 30, tzinfo=timezone.utc), None),
        )
        self.assertTupleEqual(
            parsed_result[2],
            (datetime(2021, 8, 11, 14, 1, 0, tzinfo=timezone.utc), Decimal("0")),
        )

    def test_transpose_results(self):
        sample_result_list = {}
        sample_result_list["query_name_1"] = [
            (datetime(2021, 8, 11, 15, 0, 0), Decimal("1.1")),
            (datetime(2021, 8, 11, 15, 1, 0), Decimal("1.2")),
            (datetime(2021, 8, 11, 15, 2, 0), Decimal("1.3")),
        ]

        sample_result_list["query_name_2"] = [
            (datetime(2021, 8, 11, 15, 0, 0), Decimal("0.11")),
            (datetime(2021, 8, 11, 15, 1, 0), Decimal("0.12")),
            (datetime(2021, 8, 11, 15, 2, 0), Decimal("0.13")),
        ]

        transposed_result_list = prometheus.transpose_results(sample_result_list)

        self.assertEquals(len(transposed_result_list), 4)
        self.assertEquals(len(transposed_result_list[0]), 3)
        self.assertEquals(transposed_result_list[0][0], "Datetime")
        self.assertEquals(transposed_result_list[0][1], "query_name_1")
        self.assertEquals(transposed_result_list[0][2], "query_name_2")
        self.assertIsInstance(transposed_result_list[3][0], datetime)
        self.assertIsInstance(transposed_result_list[3][1], Decimal)
        self.assertIsInstance(transposed_result_list[3][2], Decimal)

    def test_split(self):
        sample_result_list = [["Datetime", "query_1", "query_2"]]
        sample_result_list.extend(
            [
                (datetime(2021, 8, 11, 15, 0, 0), Decimal("0.11"), Decimal("0.12")),
                (datetime(2021, 8, 11, 15, 1, 0), Decimal("0"), Decimal("0.12")),
                (datetime(2021, 8, 11, 15, 2, 0), Decimal("0.1"), Decimal("0")),
            ]
        )

        # split #1 - both values 0 and 2 rows
        sample_result_list.extend(
            [
                (datetime(2021, 8, 11, 15, 3, 0), Decimal("0"), Decimal("0")),
                (datetime(2021, 8, 11, 15, 4, 0), Decimal("0"), Decimal("0")),
            ]
        )

        sample_result_list.extend(
            [
                (datetime(2021, 8, 11, 15, 5, 0), Decimal("0.11"), Decimal("0.12")),
                (datetime(2021, 8, 11, 15, 6, 0), None, Decimal("0.12")),
                (datetime(2021, 8, 11, 15, 7, 0), None, Decimal("0.14")),
                (datetime(2021, 8, 11, 15, 8, 0), Decimal("0.1"), None),
            ]
        )

        # split #2 - one value 0 other None
        sample_result_list.extend(
            [
                (datetime(2021, 8, 11, 15, 9, 0), Decimal("0"), None),
            ]
        )

        sample_result_list.extend(
            [
                (datetime(2021, 8, 11, 15, 10, 0), Decimal("0"), Decimal("8")),
            ]
        )

        # split #3 - both values None
        sample_result_list.extend([(datetime(2021, 8, 11, 15, 11, 0), None, None)])

        sample_result_list.extend(
            [
                (datetime(2021, 8, 11, 15, 12, 0), Decimal("0"), Decimal("8")),
                (datetime(2021, 8, 11, 15, 13, 0), Decimal("8.6"), Decimal("1.2")),
            ]
        )

        split_results = prometheus.split(sample_result_list)

        self.assertEquals(len(split_results), 4)
        # +1 header row
        self.assertEquals(len(split_results[0]), 3 + 1)
        self.assertEquals(len(split_results[1]), 4 + 1)
        self.assertEquals(len(split_results[2]), 1 + 1)
        self.assertEquals(len(split_results[3]), 2 + 1)

    def test_average_with_averaging_window(self):
        # -5 should be ignored, and average of 4.0 and 5.0
        # must be 4.5
        dataset = [
            (datetime(2021, 8, 11, 12, 0, 0), Decimal("-5")),
            (datetime(2021, 8, 11, 12, 0, 30), Decimal("-5")),
            (datetime(2021, 8, 11, 12, 1, 0), Decimal("4.0")),
            (datetime(2021, 8, 11, 12, 1, 30), Decimal("4.0")),
            (datetime(2021, 8, 11, 12, 2, 0), Decimal("5.0")),
            (datetime(2021, 8, 11, 12, 2, 30), Decimal("5.0")),
            (datetime(2021, 8, 11, 12, 3, 0), Decimal("-5")),
            (datetime(2021, 8, 11, 12, 3, 30), Decimal("-5")),
        ]

        self.assertEqual(prometheus.average(dataset, 60), Decimal("4.5"))

    def test_maximum(self):
        dataset = [
            (datetime(2021, 8, 11, 12, 1, 0), Decimal("4.0")),
            (datetime(2021, 8, 11, 12, 1, 30), Decimal("14.0")),
            (datetime(2021, 8, 11, 12, 2, 0), Decimal("85.0")),
            (datetime(2021, 8, 11, 12, 2, 30), Decimal("15.0")),
            (datetime(2021, 8, 11, 12, 3, 0), Decimal("-95")),
            (datetime(2021, 8, 11, 12, 3, 30), Decimal("-5")),
        ]

        self.assertEqual(prometheus.maximum(dataset), Decimal("85.0"))


if __name__ == "__main__":
    endtoend.runtests()
