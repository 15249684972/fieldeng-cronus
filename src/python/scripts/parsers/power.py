import json
from datetime import datetime
import time

#16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000
header = 'Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3'
sensors = header.split(';')

def parse(data):
  lines = data['content'].strip().replace('\r', '').split('\n')
  records = []
  for line in lines:
    if 'Global_active_power' in line: continue
    tokens = line.split(';')
    ts = int(round(time.mktime(datetime.strptime(tokens[0] + ' ' + tokens[1], '%d/%m/%Y %H:%M:00').timetuple())*1000))
    for idx, token in enumerate(tokens[2:]):
      if token == '' or '?' in token: continue
      records.append(json.dumps({'ts': ts, 'tag': sensors[idx], 'val': float(token)}))
  return '\n'.join(records)
