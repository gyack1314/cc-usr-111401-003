"""领域资料及现有接口核对。"""
import json,tempfile,unittest
from pathlib import Path
from warehouse.objects import ObjectStore
from warehouse.replica import inspect

class DomainTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.store=ObjectStore(self.root/'objects')
        self.data=json.loads(Path('data/source.json').read_text(encoding='utf-8'))
    def test_existing_reader(self):
        result=inspect(self.data,self.store,self.root)
        self.assertTrue(result)

    def test_fork_and_corrupt_object(self):
        from warehouse.replica import Inbox
        x=Inbox(self.store,self.root/'inbox');x.receive_log({'epoch':1,'offset':0,'previous':None,'manifest':[]})
        with self.assertRaises(ValueError):x.receive_log({'epoch':1,'offset':0,'previous':None,'manifest':['other']})
        with self.assertRaises(ValueError):x.receive_object(b'bad','0'*64)
