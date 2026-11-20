"""Carga de lote incremental en SQLite sin duplicar registros (sin avanzar la marca de agua)."""
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from common import DATA

archivos = sorted((DATA / 'incremental').glob('fecha_ingesta=*/nuevos.jsonl'))
if not archivos:
    raise FileNotFoundError('No se encontro el lote incremental')
archivo = archivos[-1]
base = DATA / 'servicio.db'
if not base.exists():
    raise FileNotFoundError('No se encontro servicio.db')

registros = []
ids_lote = set()
invalidos = 0
repetidos_lote = 0
with archivo.open(encoding='utf-8') as f:
    for linea in f:
        try:
            p = json.loads(linea)['payload']
            identificador = str(p['id_contrato']).strip()
            fecha = str(p['fecha_de_firma'])[:10]
            datetime.strptime(fecha, '%Y-%m-%d')
            valor = float(p['valor_del_contrato'])
            if not identificador or valor < 0:
                raise ValueError('id o valor invalido')
            if identificador in ids_lote:
                repetidos_lote += 1
                continue
            ids_lote.add(identificador)
            registros.append((identificador, fecha, valor, p.get('departamento') or 'SIN INFORMACION', p.get('nombre_entidad') or 'SIN INFORMACION', int(fecha[:4])))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            invalidos += 1

with sqlite3.connect(str(base)) as db:
    db.execute('PRAGMA busy_timeout=10000')
    respaldo = DATA / 'servicio_respaldo_incremental.db'
    if not respaldo.exists():
        with sqlite3.connect(str(respaldo)) as copia:
            db.backup(copia)
    antes = db.execute('SELECT COUNT(*) FROM contratos').fetchone()[0]
    with db:
        db.executemany('INSERT OR IGNORE INTO contratos (id_registro,fecha_contrato,valor_contrato,departamento,entidad,anio) VALUES (?,?,?,?,?,?)', registros)
        despues = db.execute('SELECT COUNT(*) FROM contratos').fetchone()[0]
        for t in ['kpi','por_departamento','por_anio','por_entidad']:
            db.execute('DELETE FROM ' + t)
        db.execute("INSERT INTO kpi SELECT 'total_contratos',COUNT(*) FROM contratos")
        db.execute("INSERT INTO kpi SELECT 'valor_total',COALESCE(SUM(valor_contrato),0) FROM contratos")
        db.execute("INSERT INTO kpi SELECT 'total_departamentos',COUNT(DISTINCT departamento) FROM contratos")
        db.execute('INSERT INTO por_departamento SELECT departamento,COUNT(*),SUM(valor_contrato) FROM contratos GROUP BY departamento')
        db.execute('INSERT INTO por_anio SELECT anio,COUNT(*),SUM(valor_contrato) FROM contratos GROUP BY anio')
        db.execute('INSERT INTO por_entidad SELECT entidad,COUNT(*),SUM(valor_contrato) FROM contratos GROUP BY entidad')
reporte = {'archivo':str(archivo),'registros_validos_en_lote':len(registros),'invalidos':invalidos,'duplicados_dentro_lote':repetidos_lote,'contratos_antes':antes,'insertados':despues-antes,'ya_existentes_en_sqlite':len(registros)-(despues-antes),'contratos_despues':despues,'idempotencia_conteo':(despues>=antes)}
(DATA/'reporte_integracion_incremental.json').write_text(json.dumps(reporte,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(reporte,ensure_ascii=False,indent=2))
