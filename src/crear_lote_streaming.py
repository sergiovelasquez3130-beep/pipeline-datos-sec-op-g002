import json,pathlib
from common import *
def main():
 manifest=json.loads((DATA/'manifiesto.json').read_text())
 path=ROOT/manifest['archivo']
 inbox=DATA/'stream_inbox';inbox.mkdir(exist_ok=True)
 # Nombre estable: ejecutar de nuevo no crea otro lote; checkpoint reconoce archivo procesado.
 dest=inbox/f'lote_{manifest["sha256"][:16]}.jsonl'
 with path.open(encoding='utf8') as src, dest.open('w',encoding='utf8') as out:
  for line in src:
   item=json.loads(line)
   out.write(json.dumps({'id_registro':item['id_registro'],'fecha_ingesta':item['fecha_ingesta']})+'\n')
 print('Entrada preparada:',dest)
if __name__=='__main__':main()
