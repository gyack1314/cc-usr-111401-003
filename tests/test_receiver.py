"""副本推进、接管协议与接收证据的核对。"""
import hashlib, tempfile, unittest
from pathlib import Path
from warehouse.objects import ObjectStore, encode
from warehouse.receiver import Replica, ForkError, replay

def digest_of(value):
    return hashlib.sha256(encode(value)).hexdigest()

class ReplicaTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.store=ObjectStore(self.root/'objects')
        bodies={'A':{'rows':[0]},'B':{'rows':[1]},'C':{'rows':[2]},'D':{'rows':[3]}}
        self.objs={name:encode(body) for name,body in bodies.items()}
        self.hash={name:hashlib.sha256(raw).hexdigest() for name,raw in self.objs.items()}
        h=self.hash
        self.logs={'L0':{'epoch':1,'offset':0,'previous':None,'manifest':[h['A']]}}
        self.logs['L1']={'epoch':1,'offset':1,'previous':digest_of(self.logs['L0']),'manifest':[h['A'],h['B']]}
        self.logs['L2']={'epoch':1,'offset':2,'previous':digest_of(self.logs['L1']),'manifest':[h['A'],h['B'],h['D']]}
        self.logs['N0']={'epoch':2,'offset':0,'previous':digest_of(self.logs['L0']),'manifest':[h['A'],h['C']]}
        self.authority=[{'epoch':1,'holder':'east'},{'epoch':2,'holder':'west','base_offset':0}]

    def make(self):
        return Replica(self.store,self.root/'replica',self.authority)

    def send(self,replica,*names):
        return [replica.receive_object(self.objs[n],self.hash[n]) for n in names]

    def test_missing_objects_hold_head(self):
        r=self.make()
        r.receive_log(self.logs['L1'])          # 目录先到，对象未到
        r.receive_log(self.logs['L0'])
        self.assertIsNone(r.status()['head'])   # 看到较新目录不宣布可读
        self.send(r,'A')
        st=r.status()
        self.assertEqual((st['head']['epoch'],st['head']['offset']),(1,0))
        self.assertEqual(st['head']['digest'],digest_of(self.logs['L0']))  # 不拼接新旧目录
        self.assertEqual(st['lag']['objects_missing'],[self.hash['B']])
        self.assertEqual(st['lag']['entries_received_not_complete'],1)
        report=r.takeover_report()
        self.assertEqual(report['takeover_position'],st['head'])
        self.assertEqual(report['missing_materials']['objects'],{'1:1':[self.hash['B']]})
        self.assertEqual(report['unreceived'],'unknown')

    def test_out_of_order_backfill_and_duplicates(self):
        r=self.make()
        r.receive_log(self.logs['L1']);r.receive_log(self.logs['L0'])
        self.send(r,'A')
        self.assertEqual(r.status()['head']['offset'],0)
        self.send(r,'B')                        # 乱序补齐后推进
        self.assertEqual(r.status()['head']['offset'],1)
        again=r.receive_log(self.logs['L1'])    # 重复到达幂等
        self.assertTrue(again['duplicate'])
        self.assertEqual(r.status()['head']['offset'],1)
        self.assertTrue(self.send(r,'B')[0]['duplicate'])
        kinds=[e['kind'] for e in r.evidence()]
        self.assertIn('duplicate_log',kinds);self.assertIn('duplicate_object',kinds)

    def test_gap_in_chain_reported_then_backfilled(self):
        r=self.make()
        self.send(r,'A','B','D')
        r.receive_log(self.logs['L0']);r.receive_log(self.logs['L2'])  # 缺 1:1
        st=r.status()
        self.assertEqual(st['head']['offset'],0)
        self.assertEqual(st['lag']['positions_missing'],['1:1'])
        r.receive_log(self.logs['L1'])          # 乱序补齐缺口
        self.assertEqual(r.status()['head']['offset'],2)

    def test_fork_halts_and_survives_restart(self):
        r=self.make()
        r.receive_log(self.logs['L0']);r.receive_log(self.logs['L1'])
        self.send(r,'A','B')
        self.assertEqual(r.status()['head']['offset'],1)
        rival=dict(self.logs['L1'],manifest=[self.hash['A']])  # 同位置不同内容
        with self.assertRaises(ForkError):r.receive_log(rival)
        st=r.status()
        self.assertTrue(st['halted'])
        self.assertEqual(st['fork']['position'],'1:1')
        self.assertEqual(st['fork']['challenger'],digest_of(rival))
        r.receive_log(self.logs['L2']);self.send(r,'D')        # 停止后不再推进
        self.assertEqual(r.status()['head']['offset'],1)
        r2=self.make()                                         # 重启仍停在分叉
        self.assertTrue(r2.status()['halted'])
        self.assertEqual(r2.status()['head']['offset'],1)
        with self.assertRaises(ForkError):r2.receive_log(rival)
        self.assertIn('fork',[e['kind'] for e in r2.evidence()])

    def test_takeover_fences_old_primary(self):
        r=self.make()
        r.receive_log(self.logs['L0']);r.receive_log(self.logs['L1'])
        self.send(r,'A','B')
        self.assertEqual(r.status()['head']['offset'],1)
        result=r.takeover()
        self.assertEqual(result['status'],'registered')
        self.assertEqual((result['epoch'],result['holder']),(2,'west'))
        self.assertEqual(r.receive_log(self.logs['L1'])['status'],'stale')   # 旧主重送
        rival=dict(self.logs['L1'],manifest=[self.hash['A']])
        self.assertEqual(r.receive_log(rival)['status'],'stale')             # 旧分支分叉只留证据
        self.assertIn('stale_fork',[e['kind'] for e in r.evidence()])
        self.assertFalse(r.status()['halted'])
        self.assertEqual(r.receive_log(self.logs['L2'])['status'],'stale')   # 旧主新位置不推进
        self.send(r,'D')
        self.assertEqual((r.status()['head']['epoch'],r.status()['head']['offset']),(1,1))
        r.receive_log(self.logs['N0'])                                       # 新世代对象未齐
        self.assertEqual((r.status()['head']['epoch'],r.status()['head']['offset']),(1,1))
        self.send(r,'C')
        self.assertEqual((r.status()['head']['epoch'],r.status()['head']['offset']),(2,0))
        self.assertEqual(r.status()['stale_received'],3)

    def test_future_epoch_waits_for_takeover(self):
        r=self.make()
        self.assertEqual(r.receive_log(self.logs['N0'])['status'],'pending_epoch')
        r.receive_log(self.logs['L0']);self.send(r,'A','C')
        self.assertEqual((r.status()['head']['epoch'],r.status()['head']['offset']),(1,0))
        self.assertEqual(r.status()['future_received'],['2:0'])
        r.takeover()
        self.assertEqual((r.status()['head']['epoch'],r.status()['head']['offset']),(2,0))

    def test_takeover_blocked_until_base_complete(self):
        authority=[{'epoch':1,'holder':'east'},{'epoch':2,'holder':'west','base_offset':1}]
        r=Replica(self.store,self.root/'replica',authority)
        r.receive_log(self.logs['L0']);r.receive_log(self.logs['L1'])
        self.send(r,'A')
        blocked=r.takeover()
        self.assertEqual(blocked['status'],'blocked')
        self.assertEqual(blocked['base_required'],'1:1')
        self.assertEqual(blocked['complete_through'],'1:0')
        self.assertEqual(blocked['missing'],[{'position':'1:1','missing_objects':[self.hash['B']]}])
        self.send(r,'B')
        done=r.takeover()
        self.assertEqual(done['status'],'registered')
        self.assertEqual((done['head']['epoch'],done['head']['offset']),(1,1))

    def test_epoch_base_must_match_authority(self):
        r=self.make()
        bad={'epoch':2,'offset':0,'previous':digest_of(self.logs['L1']),'manifest':[self.hash['A']]}
        r.receive_log(bad)                      # 基准与授权不符的新世代首项
        r.receive_log(self.logs['L0']);self.send(r,'A')
        r.takeover()
        st=r.status()
        self.assertEqual((st['head']['epoch'],st['head']['offset']),(1,0))
        self.assertEqual(st['chain_break']['position'],'2:0')
        self.assertIn('chain_break',[e['kind'] for e in r.evidence()])

    def test_read_handles_pinned_and_numbered(self):
        r=self.make()
        with self.assertRaises(ValueError):r.open_handle()     # 无完整版本不可读
        r.receive_log(self.logs['L0']);self.send(r,'A')
        h1=r.open_handle()
        self.assertEqual(h1.generation,1)
        r.receive_log(self.logs['L1']);self.send(r,'B')        # 可读头继续前进
        h2=r.open_handle()
        self.assertEqual(h2.generation,2)
        self.assertEqual(set(h1.read_json()['objects']),{self.hash['A']})            # 旧句柄读旧版本
        self.assertEqual(set(h2.read_json()['objects']),{self.hash['A'],self.hash['B']})
        r2=self.make()                                         # 重启：版本与句柄世代连续
        self.assertEqual(r2.status()['head']['offset'],1)
        h3=r2.open_handle()
        self.assertEqual(h3.generation,3)
        self.assertEqual(h3.read_json()['objects'],h2.read_json()['objects'])
        self.assertEqual(h1.read_json()['objects'],{self.hash['A']:{'rows':[0]}})

    def test_restart_resumes_pending_and_head(self):
        r=self.make()
        r.receive_log(self.logs['L0']);r.receive_log(self.logs['L1'])
        self.send(r,'A')
        first=r.status()['head']
        r2=self.make()                                         # 重启读到同一版本
        self.assertEqual(r2.status()['head'],first)
        self.assertEqual(r2.status()['lag']['objects_missing'],[self.hash['B']])
        self.send(r2,'B')                                      # 重启后继续推进
        self.assertEqual(r2.status()['head']['offset'],1)

    def test_authority_must_be_monotonic(self):
        with self.assertRaises(ValueError):
            Replica(self.store,self.root/'bad',
                    [{'epoch':2,'holder':'a'},{'epoch':2,'holder':'b','base_offset':0}])
        with self.assertRaises(ValueError):Replica(self.store,self.root/'bad',[])

    def test_replay_driver_with_takeover_event(self):
        data={'objects':{'a':{'rows':[0]},'b':{'rows':[1]},'c':{'rows':[2]}},
              'logs':[{'epoch':1,'offset':0,'previous':None,'manifest':['a']},
                      {'epoch':1,'offset':1,'previous':'log-0','manifest':['a','b']},
                      {'epoch':2,'offset':0,'previous':'log-0','manifest':['a','c']}],
              'authority':[{'epoch':1,'holder':'east'},{'epoch':2,'holder':'west','base_offset':0}],
              'delivery':[{'channel':'log','item':1},{'channel':'object','item':'a'},
                          {'channel':'log','item':0},{'channel':'log','item':2},
                          {'channel':'takeover'},
                          {'channel':'log','item':1},          # 接管后旧主重送
                          {'channel':'object','item':'c'}]}
        result=replay(data,self.store,self.root/'replica')
        st=result['status']
        self.assertEqual((st['head']['epoch'],st['head']['offset']),(2,0))
        statuses=[o.get('status') for o in result['outcomes']]
        self.assertIn('registered',statuses);self.assertIn('stale',statuses)
        self.assertEqual(st['lag']['beyond_last_received'],'unknown')
        self.assertEqual(result['takeover']['takeover_position'],st['head'])
        self.assertIsNone(result['takeover']['next'])

    def test_evidence_journal_persists(self):
        r=self.make()
        r.receive_log(self.logs['L0']);self.send(r,'A')
        r.open_handle();r.takeover();r.receive_log(self.logs['L1'])
        kinds=[e['kind'] for e in r.evidence()]
        self.assertEqual(kinds[0],'init')
        for kind in ('log_received','object_received','head_advanced','handle_opened','takeover','stale_log'):
            self.assertIn(kind,kinds)
        gens=[e['generation'] for e in r.evidence() if e['kind']=='handle_opened']
        self.assertEqual(gens,[1])
        self.assertEqual(self.make().evidence(),r.evidence())  # 重启后证据仍在

if __name__=='__main__':unittest.main()
