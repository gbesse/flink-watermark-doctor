import unittest
from watermark_doctor import simulate
class LateBudgetTests(unittest.TestCase):
 def test_cleanup_boundary_in_three_languages(self):
  for lang in ['fr','en','es']:
   events=[dict(partition='a',ts=20),dict(partition='a',ts=5)]
   x=simulate(events,10,0,10,lang=lang)['timeline'];self.assertIsNone(x[0]['late_budget_at_arrival']);self.assertEqual(x[1]['cleanup_at_watermark'],20);self.assertEqual(x[1]['late_budget_at_arrival'],0)
   y=simulate(events,10,0,11,lang=lang)['timeline'];self.assertEqual(y[1]['late_budget_at_arrival'],1)
