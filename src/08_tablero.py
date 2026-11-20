"""Tablero legible, alimentado exclusivamente por indicadores de SQLite."""
import sqlite3
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from common import DATA,ROOT

def main():
    with sqlite3.connect(str(DATA/'servicio.db')) as db:
        k = dict(db.execute('SELECT nombre,valor FROM kpi').fetchall())
        dept = pd.read_sql_query('SELECT departamento,contratos,total_valor FROM por_departamento ORDER BY total_valor DESC LIMIT 10',db)
        anio = pd.read_sql_query('SELECT anio,contratos,total_valor FROM por_anio ORDER BY anio',db)
        ent = pd.read_sql_query('SELECT entidad,contratos,total_valor FROM por_entidad ORDER BY total_valor DESC LIMIT 10',db)
        corte = db.execute('SELECT MAX(fecha_contrato) FROM contratos').fetchone()[0]
    fig = make_subplots(rows=2,cols=2,vertical_spacing=0.23,horizontal_spacing=0.18,
      specs=[[{'type':'xy'},{'type':'xy'}],[{'type':'xy'},{'type':'domain'}]],
      subplot_titles=('Contratos por año','Valor por departamento (billones COP)','Valor por año (billones COP)','Participación del top 10 de entidades'))
    years=[int(y) for y in anio['anio'].tolist()]
    fig.add_trace(go.Bar(x=[str(y) for y in years],y=anio.contratos,marker_color='#2563eb',text=anio.contratos,textposition='outside',name='Contratos',hovertemplate='%{x}: %{y:,.0f} contratos<extra></extra>'),1,1)
    dept=dept.iloc[::-1]
    fig.add_trace(go.Bar(x=dept.total_valor/1e12,y=dept.departamento,orientation='h',marker_color='#0d9488',name='Departamento',hovertemplate='%{y}<br>%{x:,.2f} billones COP<extra></extra>'),1,2)
    fig.add_trace(go.Scatter(x=[str(y) for y in years],y=anio.total_valor/1e12,mode='lines+markers',line=dict(color='#7c3aed',width=3),marker=dict(size=8),name='Valor por año',hovertemplate='%{x}: %{y:,.2f} billones COP<extra></extra>'),2,1)
    fig.add_trace(go.Pie(labels=ent.entidad,values=ent.total_valor,hole=.60,textinfo='none',showlegend=False,hovertemplate='%{label}<br>Participación: %{percent}<extra></extra>',marker=dict(line=dict(color='white',width=1))),2,2)
    fig.update_yaxes(title_text='Contratos',row=1,col=1)
    fig.update_xaxes(title_text='Billones COP',row=1,col=2)
    fig.update_yaxes(title_text='Billones COP',row=2,col=1)
    fig.update_xaxes(type='category',row=1,col=1)
    fig.update_xaxes(type='category',row=2,col=1)
    fig.update_layout(template='plotly_white',height=1050,showlegend=False,
      font=dict(family='Arial',size=12,color='#13253e'),
      margin=dict(l=110,r=65,t=185,b=100),
      title=dict(text='<b>Contratación pública en Colombia — SECOP II</b><br><sup>Fuente: datos.gov.co · Fecha máxima de firma en la muestra: {} · {:,} contratos · Valor total: {:,.0f} COP · {} categorías territoriales</sup><br><sup>Datos históricos: no representan la contratación nacional actual. Valores extraordinarios requieren revisión.</sup>'.format(corte,int(k.get('total_contratos',0)),k.get('valor_total',0),int(k.get('total_departamentos',0))),x=.03,y=.97,xanchor='left'))
    salida=ROOT/'tablero_secop.html'
    fig.write_html(str(salida),include_plotlyjs=True,full_html=True)
    print('Tablero corregido:',salida)
if __name__=='__main__':
    main()
