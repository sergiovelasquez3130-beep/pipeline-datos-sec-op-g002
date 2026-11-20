"""Exporta evidencias reales existentes y captura estado de base de servicio."""
import json,sqlite3,zipfile,datetime
from pathlib import Path
from common import ROOT,DATA

def main():
    evid=ROOT/'evidencias';evid.mkdir(exist_ok=True)
    with sqlite3.connect(str(DATA/'servicio.db')) as db:
        resumen={
          'capturado_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'contratos_sqlite':db.execute('SELECT COUNT(*) FROM contratos').fetchone()[0],
          'ids_duplicados':db.execute('SELECT COUNT(*) FROM (SELECT id_registro FROM contratos GROUP BY id_registro HAVING COUNT(*)>1)').fetchone()[0],
          'fecha_maxima_firma':db.execute('SELECT MAX(fecha_contrato) FROM contratos').fetchone()[0],
          'streaming_ventanas':db.execute('SELECT COALESCE(SUM(mensajes),0) FROM streaming_ventanas').fetchone()[0] if db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='streaming_ventanas'").fetchone()[0] else None
        }
    (evid/'verificacion_final.json').write_text(json.dumps(resumen,indent=2,ensure_ascii=False),encoding='utf-8')
    files=[evid/'verificacion_final.json',ROOT/'tablero_secop.html']
    files += [DATA/n for n in ['manifiesto_ingesta.json','reporte_calidad.json','reporte_idempotencia.json','reporte_streaming.json','reporte_integracion_incremental.json','marca_agua.json','servicio.db']]
    files += list((DATA/'raw').rglob('*.jsonl'))[:1]
    salida=ROOT/'evidencias_finales_secop.zip'
    with zipfile.ZipFile(str(salida),'w',zipfile.ZIP_DEFLATED) as z:
        for f in files:
            if f.exists():
                if f.suffix=='.jsonl':
                    with f.open(encoding='utf-8') as reader:
                        muestra=''.join(next(reader,'') for i in range(100))
                    z.writestr('evidencias/muestra_raw_100.jsonl',muestra)
                else:
                    z.write(str(f),str(f.relative_to(ROOT)))
    print('Verificacion final:',json.dumps(resumen,ensure_ascii=False))
    print('Archivo evidencias:',salida)
if __name__=='__main__':main()
