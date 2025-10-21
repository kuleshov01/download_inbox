import unittest
from datetime import datetime

from main import outlook_us_datetime_str, build_received_time_filter


class OutlookDateFormattingTests(unittest.TestCase):
    def test_midnight_format(self):
        dt = datetime(2025, 10, 1, 0, 0, 0)
        self.assertEqual(outlook_us_datetime_str(dt), "10/01/2025 12:00 AM")

    def test_afternoon_format(self):
        dt = datetime(2025, 5, 9, 15, 7, 0)
        self.assertEqual(outlook_us_datetime_str(dt), "05/09/2025 03:07 PM")


class ReceivedTimeFilterTests(unittest.TestCase):
    def test_filter_inclusive_end_covers_full_day(self):
        start = datetime(2024, 3, 1, 0, 0, 0)
        end = datetime(2024, 3, 5, 23, 59, 59)

        filter_str, end_exclusive = build_received_time_filter(start, end)

        self.assertIn("[ReceivedTime] >= '03/01/2024 12:00 AM'", filter_str)
        self.assertIn("[ReceivedTime] < '03/06/2024 12:00 AM'", filter_str)
        self.assertEqual(end_exclusive, datetime(2024, 3, 6, 0, 0, 0))

    def test_filter_raises_when_start_after_end(self):
        start = datetime(2024, 3, 10, 0, 0, 0)
        end = datetime(2024, 3, 5, 23, 59, 59)

        with self.assertRaises(ValueError):
            build_received_time_filter(start, end)


if __name__ == "__main__":
    unittest.main()
