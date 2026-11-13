"""现有资料读取入口。"""
import json,tempfile,argparse
from pathlib import Path
from .objects import ObjectStore
from .replica import inspect

def main():
    parser=argparse.ArgumentParser(description='读取并核对本地资料')
    parser.add_argument('--source',default='data/source.json')
    parser.add_argument('--replica',action='store_true',help='运行副本推进与接管报告')
    args=parser.parse_args()
    data=json.loads(Path(args.source).read_text(encoding='utf-8'))
    with tempfile.TemporaryDirectory() as temporary:
        root=Path(temporary);store=ObjectStore(root/'objects')
        if args.replica:
            from .receiver import replay
            result=replay(data,store,root/'replica')
            print(json.dumps({'status':result['status'],'takeover':result['takeover']},ensure_ascii=False,sort_keys=True))
        else:
            result=inspect(data,store,root)
            print(json.dumps(result,ensure_ascii=False,sort_keys=True))
if __name__=='__main__':main()
