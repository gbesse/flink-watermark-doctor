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
        self.assertEqual(
            [x["watermark_after"] for x in result["timeline"]], [1, 1, 1, 20]
        )
        self.assertEqual(
            result["timeline"][-1]["fired"], [{"window_end": 10, "count": 2}]
        )

    def test_too_late_event_is_dropped(self):
        result = simulate(
            [
                {"partition": "A", "ts": 1},
                {"partition": "A", "ts": 20},
                {"partition": "A", "ts": 2},
            ],
            10,
            0,
            0,
        )
        self.assertEqual(result["timeline"][-1]["result"], "too late")
        self.assertEqual(result["windows"][0]["count"], 1)

    def test_late_update_with_grace(self):
        result = simulate(
            [
                {"partition": "A", "ts": 1},
                {"partition": "A", "ts": 10},
                {"partition": "A", "ts": 2},
            ],
            10,
            0,
            5,
        )
        self.assertEqual(result["timeline"][-1]["result"], "late update")
        self.assertEqual(result["windows"][0]["count"], 2)


class ColdPartitionTests(unittest.TestCase):
    def test_unseen_declared_partition_holds_watermark_until_idle(self):
        result = simulate(
            [
                {"partition": "A", "ts": 1},
                {"partition": "A", "ts": 20},
                {"partition": "B", "type": "idle"},
            ],
            10,
            0,
            0,
            partitions=["A", "B"],
        )
        self.assertEqual(
            [r["watermark_after"] for r in result["timeline"]], [None, None, 20]
        )
        self.assertEqual(result["timeline"][1]["waiting_partitions"], ["B"])
        self.assertEqual(result["timeline"][1]["fired"], [])
        self.assertEqual(
            result["timeline"][2]["fired"], [{"window_end": 10, "count": 1}]
        )

    def test_resume_without_first_timestamp_stalls_without_regressing_watermark(self):
        result = simulate(
            [
                {"partition": "B", "type": "idle"},
                {"partition": "A", "ts": 20},
                {"partition": "B", "type": "resume"},
                {"partition": "A", "ts": 50},
                {"partition": "B", "ts": 30},
            ],
            10,
            0,
            0,
            partitions=["A", "B"],
        )
        self.assertEqual(
            [r["watermark_after"] for r in result["timeline"]], [None, 20, 20, 20, 30]
        )
        self.assertEqual(result["timeline"][3]["waiting_partitions"], ["B"])
        self.assertEqual(result["timeline"][4]["waiting_partitions"], [])

    def test_validation_of_declared_partition_and_event_contract(self):
        for partitions in ([], ["A", "A"], [""], [True], "A"):
            with self.assertRaises(ValueError):
                simulate([], 10, 0, 0, partitions=partitions)
        with self.assertRaises(ValueError):
            simulate([{"partition": "unknown", "ts": 1}], 10, 0, 0, partitions=["A"])
        for event in (
            {"partition": "", "ts": 1},
            {"partition": "A", "ts": True},
            {"partition": "A", "type": "typo"},
        ):
            with self.assertRaises(ValueError):
                simulate([event], 10, 0, 0)
        for values in ((True, 0, 0), (10, 0.5, 0), (10, 0, -1)):
            with self.assertRaises(ValueError):
                simulate([], *values)

    def test_three_languages_preserve_identifiers(self):
        from watermark_doctor import LABELS

        for lang in LABELS:
            result = simulate(
                [{"partition": "Usuario", "ts": 1}],
                10,
                0,
                0,
                partitions=["Usuario"],
                lang=lang,
            )
            self.assertEqual(result["timeline"][0]["partition"], "Usuario")
            self.assertEqual(result["timeline"][0]["result"], LABELS[lang]["accepted"])


class LateEmissionTests(unittest.TestCase):
    def test_late_emissions_replace_full_count_and_too_late_never_emits(self):
        for lang in ("fr", "en", "es"):
            result = simulate([
                {"partition":"A","ts":1},
                {"partition":"A","ts":10},
                {"partition":"A","ts":2},
                {"partition":"A","ts":3},
                {"partition":"A","ts":15},
                {"partition":"A","ts":4},
            ], 10, 0, 5, lang=lang)
            rows = result["timeline"]
            self.assertEqual(rows[1]["fired"], [{"window_end":10,"count":1}])
            self.assertEqual(rows[2]["late_updates"], [{"window_end":10,"count":2}])
            self.assertEqual(rows[3]["late_updates"], [{"window_end":10,"count":3}])
            self.assertEqual(rows[2]["fired"], [])
            self.assertEqual(rows[-1]["late_updates"], [])
            self.assertEqual(result["windows"][0]["count"],3)

    def test_update_lists_are_independent_per_step(self):
        rows = simulate([{"partition":"A","ts":1},{"partition":"A","ts":20}],10,0,0)["timeline"]
        rows[0]["late_updates"].append({"window_end":-1,"count":99})
        self.assertEqual(rows[1]["late_updates"],[])

if __name__ == "__main__":
    unittest.main()
