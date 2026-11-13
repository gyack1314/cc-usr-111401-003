"""复制通道消息与正文对象的独立落地。"""
from .catalog import Catalog
from .objects import encode
import hashlib

class Inbox:
    def __init__(self,store,root):self.store=store;self.logs=Catalog(root/'logs')
    def receive_log(self,item):
        if item['epoch']<1 or item['offset']<0:raise ValueError('日志位置非法')
        digest=self.store.put_json(item);name=f"{item['epoch']}:{item['offset']}"
        current=self.logs.read().get(name)
        if current and current!=digest:raise ValueError('日志位置分叉')
        if not current:self.logs.compare_update(name,None,digest)
        return digest
    def receive_object(self,raw,expected):
        if hashlib.sha256(raw).hexdigest()!=expected:raise ValueError('通道正文损坏')
        return self.store.put(raw)

def inspect(data,store,root):
    inbox=Inbox(store,root/'inbox')
    blobs={name:encode(body) for name,body in data['objects'].items()}
    hashes={name:hashlib.sha256(raw).hexdigest() for name,raw in blobs.items()}
    logs=[];log_hashes={}
    for i,item in enumerate(data['logs']):
        converted=dict(item,previous=log_hashes.get(item['previous']),manifest=[hashes[n] for n in item['manifest']])
        logs.append(converted);log_hashes[f'log-{i}']=hashlib.sha256(encode(converted)).hexdigest()
    received=[]
    for event in data['delivery']:
        if event['channel']=='log':inbox.receive_log(logs[event['item']])
        elif event['channel']=='object':
            name=event['item'];inbox.receive_object(blobs[name],hashes[name]);received.append(hashes[name])
        else:raise ValueError('未知通道')
    return {'received_positions':inbox.logs.read(),'received_objects':received,'object_names':hashes}
