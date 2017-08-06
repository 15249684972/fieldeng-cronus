import json
import time

# Testing
#data = {}
#with open('/Users/randy/data/gas/1/test1.txt', 'r') as fp:
#  data['content'] = fp.read()
header = 'Time (seconds)'

def parse(data):
  lines = data['content'].strip().split('\n')
  records = [] 
  for line in lines:
    if header in line: continue
    tokens = line.split()
    ts = int(round(time.time()*1000 + float(tokens[0])))
    records.append(json.dumps({'ts': ts, 'tag': 'methane_conc', 'val': float(tokens[1])}))
    records.append(json.dumps({'ts': ts, 'tag': 'ethylene_conc', 'val': float(tokens[2])}))
    for idx, token in enumerate(tokens[3:]):
      records.append(json.dumps({'ts': ts, 'tag': 'sensor_' + str(idx), 'val': float(token)}))
  return '\n'.join(records)
