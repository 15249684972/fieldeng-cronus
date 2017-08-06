import json
from datetime import datetime
import time

attributes = ['filename']

# Testing
#data = {'filename': '/Users/randy/data/heartrate/hr.11839'}
#with open(data['filename'], 'r') as fp:
#  data['content'] = fp.read()

def parse(data):
  lines = data['content'].strip().replace('\r', '').split('\n')
  start = int(time.time())*1000
  tag = data['filename'].split('/')[-1]
  records = []
  for line in lines:
    ts = start + len(records) * 500
    records.append(json.dumps({'ts': ts, 'tag': tag, 'val': float(line)}))
  return '\n'.join(records)
