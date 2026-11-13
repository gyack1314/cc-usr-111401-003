"""副本推进、可读头、读句柄与接管协议。

在 Inbox 独立落地之上维护：
- 可读头只指向当前授权世代中从基准起连续、前驱相符、对象闭包全部
  通过摘要校验的最后日志位置；对象未齐时保留上一版，不拼接新旧目录；
- 读句柄按登记世代固定版本，世代号持久、重启连续；
- 接管按本地授权记录登记单调递增世代，基准提交必须本地已完整；
  登记后旧世代日志只留证据（stale），不再推进正式分支；
- 同一日志位置出现不同内容即登记分叉并停止推进，不按到达先后选赢家；
- evidence.jsonl 按序保留接收证据，落后情况明确报告，未收到部分
  一律标记 unknown，不承诺零损失恢复。
"""
from pathlib import Path
import json, os
from .catalog import Catalog
from .objects import encode
from .replica import Inbox, compile_source

class ForkError(ValueError):
    """同一日志位置收到不同内容。"""

def _is_int(value):
    return isinstance(value,int) and not isinstance(value,bool)

def _is_digest(value):
    return isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value)

def _position(epoch,offset):
    return f"{epoch}:{offset}"

def _parse(position):
    epoch,offset=position.split(':')
    return int(epoch),int(offset)

class ReadHandle:
    """登记世代固定的读句柄，读取不受可读头推进影响。"""
    def __init__(self,store,generation,epoch,offset,digest):
        self.store=store;self.generation=generation
        self.epoch=epoch;self.offset=offset;self.digest=digest
    def info(self):
        return {'generation':self.generation,'epoch':self.epoch,'offset':self.offset,'digest':self.digest}
    def read(self):
        entry=self.store.json(self.digest)
        return {'epoch':self.epoch,'offset':self.offset,'digest':self.digest,
                'objects':{d:self.store.get(d) for d in entry['manifest']}}
    def read_json(self):
        result=self.read()
        return dict(result,objects={d:json.loads(raw) for d,raw in result['objects'].items()})

class Replica:
    """湖表目录及不可变文件的一致副本接收层。"""
    def __init__(self,store,root,authority):
        self.store=store
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True)
        self.authority=list(authority)
        self._validate_authority()
        self.inbox=Inbox(store,self.root/'inbox')
        self.state=Catalog(self.root/'state')
        self.journal_path=self.root/'evidence.jsonl'
        self._seq=0
        if self.journal_path.exists():
            with self.journal_path.open('rb') as stream:
                self._seq=sum(1 for line in stream if line.strip())
        self._breaks=set()
        saved=self.state.read().get('replica')
        if saved is None:
            saved={'epoch':self.authority[0]['epoch'],'applied':[self.authority[0]],
                   'head':None,'handles_issued':0,'stale_received':0,'fork':None}
            self.state.compare_update('replica',None,saved)
            self._journal('init',epoch=saved['epoch'],holder=self.authority[0]['holder'])
        elif saved['applied']!=[dict(r) for r in self.authority[:len(saved['applied'])]]:
            raise ValueError('本地授权记录与已登记世代不符')

    def _validate_authority(self):
        if not self.authority:raise ValueError('缺少授权记录')
        epochs=[]
        for i,record in enumerate(self.authority):
            epoch,holder=record.get('epoch'),record.get('holder')
            if not _is_int(epoch) or epoch<1:raise ValueError('授权世代非法')
            if not isinstance(holder,str) or not holder:raise ValueError('授权持有者非法')
            if i and (not _is_int(record.get('base_offset')) or record['base_offset']<0):
                raise ValueError('授权基准非法')
            epochs.append(epoch)
        if any(b<=a for a,b in zip(epochs,epochs[1:])):raise ValueError('授权世代必须递增')

    def _current(self):
        return self.state.read()['replica']

    def _save(self,**changes):
        old=self._current()
        self.state.compare_update('replica',old,dict(old,**changes))

    def _journal(self,kind,**fields):
        self._seq+=1
        event=dict({'seq':self._seq,'kind':kind},**fields)
        with self.journal_path.open('ab') as stream:
            stream.write(encode(event)+b'\n');stream.flush();os.fsync(stream.fileno())
        return event

    def evidence(self):
        """按序读取已保留的接收证据。"""
        if not self.journal_path.exists():return []
        return [json.loads(line) for line in self.journal_path.read_text(encoding='utf-8').splitlines() if line.strip()]

    def _has(self,digest):
        return _is_digest(digest) and (self.store.root/digest).exists()

    def _validate_log(self,item):
        if not isinstance(item,dict) or set(item)!={'epoch','offset','previous','manifest'}:
            raise ValueError('日志项字段不符')
        if not _is_int(item['epoch']) or item['epoch']<1:raise ValueError('日志世代非法')
        if not _is_int(item['offset']) or item['offset']<0:raise ValueError('日志位置非法')
        if item['previous'] is not None and not _is_digest(item['previous']):
            raise ValueError('日志前驱非法')
        if not isinstance(item['manifest'],list) or any(not _is_digest(m) for m in item['manifest']):
            raise ValueError('日志对象闭包非法')

    def receive_log(self,item):
        """落地日志项并按授权世代推进；旧世代只留证据，分叉即停止。"""
        self._validate_log(item)
        st=self._current()
        pos=_position(item['epoch'],item['offset'])
        digest=self.store.put_json(item)
        if item['epoch']<st['epoch']:
            held=self.inbox.logs.read().get(pos)
            kind='stale_fork' if held is not None and held!=digest else 'stale_log'
            self._save(stale_received=st['stale_received']+1)
            self._journal(kind,position=pos,digest=digest,current_epoch=st['epoch'])
            return {'status':'stale','position':pos,'current_epoch':st['epoch'],'head':st['head']}
        known=self.inbox.logs.read().get(pos)
        try:
            self.inbox.receive_log(item)
        except ValueError:
            fork={'position':pos,'held':known,'challenger':digest}
            if st['fork'] is None:self._save(fork=fork)
            self._journal('fork',**fork)
            raise ForkError(f"日志位置分叉: {pos}")
        duplicate=known==digest
        self._journal('duplicate_log' if duplicate else 'log_received',position=pos,digest=digest)
        if item['epoch']>st['epoch']:
            return {'status':'pending_epoch','position':pos,'current_epoch':st['epoch'],'head':st['head']}
        if st['fork'] is not None:
            return {'status':'halted','fork':st['fork'],'head':st['head']}
        head=self._advance()
        return {'status':'ok','position':pos,'duplicate':duplicate,'advanced':head!=st['head'],'head':head}

    def receive_object(self,raw,expected):
        """校验并落地对象正文，随后尝试推进可读头。"""
        st=self._current()
        present=self._has(expected)
        try:
            digest=self.inbox.receive_object(raw,expected)
        except ValueError:
            self._journal('object_corrupt',expected=expected if isinstance(expected,str) else None)
            raise
        self._journal('duplicate_object' if present else 'object_received',digest=digest)
        if st['fork'] is not None:
            return {'status':'halted','fork':st['fork'],'head':st['head']}
        head=self._advance()
        return {'status':'ok','digest':digest,'duplicate':present,'advanced':head!=st['head'],'head':head}

    def _epoch_index(self,epoch):
        for i,record in enumerate(self.authority):
            if record['epoch']==epoch:return i
        raise ValueError('世代不在授权记录中')

    def _scan(self,epoch):
        """扫描世代链：最长完整前缀、待补位置、缺口与断链。"""
        positions=self.inbox.logs.read()
        index=self._epoch_index(epoch)
        base_digest=None;base_known=True
        if index:
            base_pos=_position(self.authority[index-1]['epoch'],self.authority[index]['base_offset'])
            base_digest=positions.get(base_pos)
            base_known=base_digest is not None
        received=sorted(off for ep,off in (_parse(p) for p in positions) if ep==epoch)
        complete=None;chain_break=None
        if base_known:
            previous,offset=base_digest,0
            while True:
                digest=positions.get(_position(epoch,offset))
                if digest is None:break
                entry=self.store.json(digest)
                if entry['previous']!=previous:
                    chain_break={'position':_position(epoch,offset),
                                 'expected_previous':previous,'found':entry['previous']}
                    break
                if any(not self._has(m) for m in entry['manifest']):break
                complete=(offset,digest)
                previous,offset=digest,offset+1
        complete_offset=complete[0] if complete else -1
        pending=[]
        for off in received:
            if off>complete_offset:
                digest=positions[_position(epoch,off)]
                entry=self.store.json(digest)
                missing=[m for m in entry['manifest'] if not self._has(m)]
                pending.append({'position':_position(epoch,off),'digest':digest,'missing_objects':missing})
        last=received[-1] if received else -1
        gaps=[_position(epoch,off) for off in range(complete_offset+1,last+1)
              if _position(epoch,off) not in positions]
        return {'complete':complete,'chain_break':chain_break,'pending':pending,
                'gaps':gaps,'received':received}

    def _advance(self):
        """把可读头推进到最后一个完整版本；分叉停止时保持不动。"""
        st=self._current()
        if st['fork'] is not None:return st['head']
        scan=self._scan(st['epoch'])
        chain_break=scan['chain_break']
        if chain_break and chain_break['position'] not in self._breaks:
            self._breaks.add(chain_break['position'])
            self._journal('chain_break',**chain_break)
        if scan['complete'] is None:return st['head']
        offset,digest=scan['complete']
        head=st['head']
        if head is None or (st['epoch'],offset)>(head['epoch'],head['offset']):
            head={'epoch':st['epoch'],'offset':offset,'digest':digest}
            self._save(head=head)
            self._journal('head_advanced',epoch=head['epoch'],offset=offset,digest=digest)
        return head

    def takeover(self):
        """登记本地授权记录中的下一个单调世代；基准未完整则报告尚缺材料。"""
        st=self._current()
        if st['fork'] is not None:
            return {'status':'halted','fork':st['fork']}
        index=len(st['applied'])
        if index>=len(self.authority):
            return {'status':'no_authority','epoch':st['epoch']}
        record=self.authority[index]
        base_pos=_position(st['epoch'],record['base_offset'])
        scan=self._scan(st['epoch'])
        complete_offset=scan['complete'][0] if scan['complete'] else -1
        if record['base_offset']>complete_offset:
            pending={p['position']:p for p in scan['pending']}
            missing=[]
            for off in range(complete_offset+1,record['base_offset']+1):
                pos=_position(st['epoch'],off)
                info=pending.get(pos)
                if info is None:missing.append({'position':pos,'entry':'unreceived'})
                elif info['missing_objects']:missing.append({'position':pos,'missing_objects':info['missing_objects']})
                else:missing.append({'position':pos,'entry':'waiting_on_predecessor'})
            self._journal('takeover_blocked',base=base_pos,missing=missing)
            return {'status':'blocked','base_required':base_pos,
                    'complete_through':None if complete_offset<0 else _position(st['epoch'],complete_offset),
                    'missing':missing}
        base_digest=self.inbox.logs.read()[base_pos]
        self._save(epoch=record['epoch'],applied=st['applied']+[record])
        self._journal('takeover',epoch=record['epoch'],holder=record['holder'],
                      base=base_pos,base_digest=base_digest)
        head=self._advance()
        return {'status':'registered','epoch':record['epoch'],'holder':record['holder'],
                'base':base_pos,'base_digest':base_digest,'head':head}

    def open_handle(self):
        """按当前可读头开立读句柄，世代号持久递增。"""
        st=self._current()
        if st['head'] is None:raise ValueError('尚无完整版本可读')
        generation=st['handles_issued']+1
        self._save(handles_issued=generation)
        head=st['head']
        self._journal('handle_opened',generation=generation,epoch=head['epoch'],
                      offset=head['offset'],digest=head['digest'])
        return ReadHandle(self.store,generation,head['epoch'],head['offset'],head['digest'])

    def status(self):
        """当前可读头、待补材料与明确的复制落后情况。"""
        st=self._current()
        scan=self._scan(st['epoch'])
        positions=self.inbox.logs.read()
        future=sorted((p for p in positions if _parse(p)[0]>st['epoch']),key=_parse)
        missing_objects=sorted({m for p in scan['pending'] for m in p['missing_objects']})
        return {'epoch':st['epoch'],'holder':st['applied'][-1]['holder'],
                'head':st['head'],'halted':st['fork'] is not None,'fork':st['fork'],
                'chain_break':scan['chain_break'],'pending':scan['pending'],
                'future_received':future,'stale_received':st['stale_received'],
                'handles_issued':st['handles_issued'],
                'lag':{'entries_received_not_complete':len(scan['pending']),
                       'objects_missing':missing_objects,
                       'positions_missing':scan['gaps'],
                       'last_received':None if not scan['received'] else _position(st['epoch'],scan['received'][-1]),
                       'beyond_last_received':'unknown'}}

    def takeover_report(self):
        """主端失联后可接管的准确位置与尚缺材料；未收到部分不承诺恢复。"""
        st=self._current()
        scan=self._scan(st['epoch'])
        report={'takeover_position':st['head'],'epoch':st['epoch'],
                'missing_materials':{
                    'positions_missing':scan['gaps'],
                    'objects':{p['position']:p['missing_objects'] for p in scan['pending'] if p['missing_objects']}},
                'unreceived':'unknown'}
        index=len(st['applied'])
        if index<len(self.authority):
            record=self.authority[index]
            complete_offset=scan['complete'][0] if scan['complete'] else -1
            report['next']={'epoch':record['epoch'],'holder':record['holder'],
                            'base_required':_position(st['epoch'],record['base_offset']),
                            'base_complete':record['base_offset']<=complete_offset}
        else:
            report['next']=None
        if st['fork'] is not None:report['fork']=st['fork']
        return report

def replay(data,store,root):
    """按合成投递事件驱动副本：log/object 落地推进，takeover 登记下一授权世代。"""
    blobs,hashes,logs=compile_source(data)
    replica=Replica(store,root,data['authority'])
    outcomes=[]
    for event in data['delivery']:
        channel=event['channel']
        if channel=='log':outcomes.append(replica.receive_log(logs[event['item']]))
        elif channel=='object':
            name=event['item'];outcomes.append(replica.receive_object(blobs[name],hashes[name]))
        elif channel=='takeover':outcomes.append(replica.takeover())
        else:raise ValueError('未知通道')
    return {'replica':replica,'outcomes':outcomes,
            'status':replica.status(),'takeover':replica.takeover_report()}
