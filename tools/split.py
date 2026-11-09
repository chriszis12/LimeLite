#!/usr/bin/env python3
"""Split a monolithic LimeLite html into public/index.html + public/game-data.json.
usage: python3 tools/split.py path/to/yardNN.html"""
import sys,re,hashlib,os
src=open(sys.argv[1],encoding='utf-8').read()
m=re.search(r'(<script type="application/json" id="game-data")>(.*?)</script>',src,re.S)
if not m: sys.exit('no game-data block found')
data=m.group(2)
h=hashlib.sha1(data.encode()).hexdigest()[:10]
root=os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','public')
os.makedirs(root,exist_ok=True)
open(os.path.join(root,'game-data.json'),'w',encoding='utf-8').write(data)
out=src[:m.start()]+m.group(1)+' data-src="game-data.json?v=%s"></script>'%h+src[m.end():]
open(os.path.join(root,'index.html'),'w',encoding='utf-8').write(out)
print('ok',len(out)//1024,'KB html',len(data)//1024,'KB data')
