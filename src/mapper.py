#!/usr/bin/env python3
import sys,json
for line in sys.stdin:
 try:
  obj=json.loads(line)
  if obj.get('id_registro'):print('contratos\t1')
 except (ValueError,KeyError):pass
