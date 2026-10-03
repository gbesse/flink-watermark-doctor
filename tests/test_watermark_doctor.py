import unittest

from watermark_doctor import simulate


class WatermarkTests(unittest.TestCase):
    def test_slow_partition_holds_global_watermark_until_idle(self):
        events = [
            {"partition": "A", "ts": 1},
            {"partition": "B", "ts": 1},
            {"partition": "A", "ts": 20},
            {"partition": "B", "type": "idle"},
        ]
        result = simulate(events, 10, 0, 0)
        self.assertEqual([x["watermark_after"] for x in result["timeline"]], [1, 1, 1, 20])
        self.assertEqual(result["timeline"][-1]["fired"], [{"window_end": 10, "count": 2}])

    def test_too_late_event_is_dropped(self):
        result = simulate([
            {"partition": "A", "ts": 1},
            {"partition": "A", "ts": 20},
            {"partition": "A", "ts": 2},
        ], 10, 0, 0)
        self.assertEqual(result["timeline"][-1]["result"], "too late")
        self.assertEqual(result["windows"][0]["count"], 1)

    def test_late_update_with_grace(self):
        result = simulate([
            {"partition": "A", "ts": 1},
            {"partition": "A", "ts": 10},
            {"partition": "A", "ts": 2},
        ], 10, 0, 5)
        self.assertEqual(result["timeline"][-1]["result"], "late update")
        self.assertEqual(result["windows"][0]["count"], 2)


if __name__ == "__main__":
    unittest.main()
