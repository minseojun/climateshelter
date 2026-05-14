import json
import os

with open('trees_cache.json') as f:
    data = json.load(f)

slim = [[round(t['lat'],5), round(t['lng'],5)] for t in data]

with open('trees_slim.json', 'w') as f:
    json.dump(slim, f, separators=(',',':'))

print(f'원본: {os.path.getsize("trees_cache.json")/1024/1024:.1f}MB')
print(f'압축: {os.path.getsize("trees_slim.json")/1024/1024:.1f}MB')
print(f'건수: {len(slim)}개')