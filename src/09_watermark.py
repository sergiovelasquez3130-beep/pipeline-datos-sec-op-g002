# Una carga incremental utiliza fecha_de_firma como marca de agua más solapamiento
# para capturar correcciones tardías. La PK de SQLite elimina duplicados.
import json,sqlite3
from common import *

def main():
 with sqlite3.connect(DATA/'servicio.db') as db:
  date=db.execute('SELECT MAX(fecha_contrato) FROM contratos').fetchone()[0]
 dump(DATA/'marca_agua.json',{'campo':'fecha_de_firma','ultima_fecha':date,'politica':'Para próximas cargas consultar API desde ultima_fecha - 7 días; UPSERT por id_contrato en servicio. No equivale a CDC completo.'})
 print('Marca de agua:',date)
if __name__=='__main__':main()
