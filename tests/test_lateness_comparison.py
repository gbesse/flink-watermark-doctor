import unittest
from watermark_doctor import compare_lateness
class LatenessComparisonTests(unittest.TestCase):
 def test_same_trace_exposes_which_policy_drops_a_late_update(self):
  events=[{'partition':'A','ts':1},{'partition':'A','ts':12},{'partition':'A','ts':2}]
  for lang in ('fr','en','es'):
   r=compare_lateness(iter(events),10,0,[0,5],lang=lang);self.assertEqual(r['policies'][0]['dropped_steps'],[3]);self.assertEqual(r['policies'][1]['dropped_steps'],[]);self.assertEqual(r['policies'][1]['accepted_event_count'],3)
 def test_invalid_policy_list(self):
  for values in ([],[0,0],[True],[1,-1]):
   with self.assertRaises(ValueError):compare_lateness([],10,0,values)
