import unittest,tempfile
from pathlib import Path
from watermark_doctor import load_events
class JsonContract(unittest.TestCase):
 def test_ambiguous_records_are_rejected_with_line_context(self):
  for line in ['{"partition":"a","partition":"b","ts":1}','{"partition":"a","ts":1,"ts":2}','{"partition":"a","ts":1,"meta":{"x":1,"x":2}}','{"partition":"a","ts":1,"meta":NaN}']:
   with tempfile.TemporaryDirectory()as d:
    p=Path(d)/'events.jsonl';p.write_text('\n'+line+'\n')
    with self.assertRaises(ValueError)as e:load_events(p)
    self.assertIn(':2:',str(e.exception))
    for text in ['Invalid parameters','Paramètres','Parámetros']:self.assertIn(text,str(e.exception))
 def test_blank_lines_and_valid_records_remain_supported(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)/'events.jsonl';p.write_text('\n{"partition":"a","ts":1}\n');self.assertEqual(load_events(p),[{'partition':'a','ts':1}])
