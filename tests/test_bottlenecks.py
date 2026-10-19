import unittest
from watermark_doctor import simulate
class BottleneckTests(unittest.TestCase):
 def run_model(self, events, **kw): return simulate(events, 10, 2, 0, **kw)['timeline']
 def test_tied_and_unique_limiting_partitions(self):
  t=self.run_model([{'partition':'a','ts':10},{'partition':'b','ts':10},{'partition':'a','ts':20}])
  self.assertEqual(t[1]['limiting_partitions'],['a','b']);self.assertEqual(t[2]['limiting_partitions'],['b']);self.assertEqual(t[2]['partition_candidates'],{'a':18,'b':8});self.assertEqual(t[2]['progress_reason'],'held');self.assertFalse(t[2]['watermark_advanced'])
 def test_declared_cold_partition_overrides_observed_minimum(self):
  t=self.run_model([{'partition':'a','ts':10},{'partition':'b','type':'idle'}],partitions=['a','b'])
  self.assertEqual(t[0]['progress_reason'],'waiting');self.assertIsNone(t[0]['watermark_after']);self.assertEqual(t[1]['progress_reason'],'advanced');self.assertEqual(t[1]['watermark_after'],8)
 def test_resuming_old_partition_does_not_rewind_watermark(self):
  t=self.run_model([{'partition':'a','ts':10},{'partition':'b','ts':20},{'partition':'a','type':'idle'},{'partition':'a','type':'resume'}])
  self.assertEqual(t[-1]['limiting_partitions'],['a']);self.assertEqual(t[-1]['watermark_after'],18);self.assertEqual(t[-1]['progress_reason'],'held')
 def test_all_idle_and_explanations_in_each_language(self):
  for lang in ['fr','en','es']:
   t=self.run_model([{'partition':'a','ts':10},{'partition':'a','type':'idle'}],lang=lang)
   self.assertEqual(t[-1]['progress_reason'],'idle');self.assertEqual(t[-1]['limiting_partitions'],[]);self.assertTrue(t[-1]['explanation']);self.assertEqual(t[-1]['watermark_after'],8)
