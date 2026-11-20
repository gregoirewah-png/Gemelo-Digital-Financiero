"""Asistente con evidencia SQL real y herramientas de dominio con parámetros."""
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from groq import Groq
from psycopg2.extras import RealDictCursor

from src.common.config import ROOT, db_connect, redact
from src.ml.monte_carlo import MonteCarloConfig


def load_data_catalog():
    try:
        catalog = json.loads(Path(os.getenv('DATA_CATALOG_PATH', str(ROOT/'configs/data_catalog.json'))).read_text())
        return catalog, None
    except (OSError, ValueError) as exc:
        return {}, str(exc)


def query_gold(dataset, view='summary', customer_id=None, date_from=None, date_to=None, source_run_id=None):
    """Consultas parametrizadas de los productos Gold publicados e históricos."""
    params=[]
    definitions={}
    if dataset=='dataset1':
        queries={
            'monthly':('SELECT * FROM serving.monthly_cashflow WHERE true',' ORDER BY period_month LIMIT 1200'),
            'summary':('SELECT COUNT(*) AS months,MIN(period_month) AS first_month,MAX(period_month) AS last_month,'
                       'SUM(total_income) AS income,SUM(total_expenses) AS expenses,SUM(net_cash_flow) AS net_cash_flow,'
                       'SUM(transaction_count) AS transactions FROM serving.monthly_cashflow WHERE true',''),
            'categories':('SELECT category,SUM(expense_total) AS expense_total,SUM(expense_transaction_count) AS transactions '
                          'FROM serving.category_spending WHERE true',' GROUP BY category ORDER BY expense_total DESC LIMIT 50'),
            'category_monthly':('SELECT * FROM serving.category_spending WHERE true',' ORDER BY period_month,category LIMIT 1200'),
        }
        if view not in queries:
            raise ValueError('Vista de movimientos no disponible')
        query,suffix=queries[view]
        if date_from:
            query+=' AND period_month >= %s::date'
            params.append(date_from)
        if date_to:
            query+=' AND period_month <= %s::date'
            params.append(date_to)
        query+=suffix
    elif dataset=='dataset2':
        if customer_id:
            view='customer'
            query='SELECT cust_id,features FROM serving.credit_cards WHERE cust_id=%s'
            params=[customer_id]
        elif view=='anomalies':
            query=('SELECT cust_id,anomaly_score,isolation_score,decision_score,credit_utilization,model_run_id,scored_at '
                   'FROM serving.credit_card_anomalies WHERE is_anomaly ORDER BY anomaly_score DESC LIMIT 20')
        else:
            view='segments'
            query=('SELECT cluster,COUNT(*) AS customers,AVG(credit_utilization_pct) AS mean_utilization_pct,'
                   'SUM(balance) AS total_balance,SUM(credit_limit) AS total_credit_limit,'
                   'SUM(purchases) AS purchases,COUNT(*) FILTER(WHERE is_anomaly) AS anomalies '
                   'FROM serving.credit_cards GROUP BY cluster ORDER BY cluster')
    elif dataset=='dataset3':
        definitions={'savings_capacity':'Ingreso menos gastos menos cuota mensual del préstamo. La fórmula asume gastos sin incluir la cuota.',
                     'mean_cash_before_loan':'Ingreso menos gastos, antes de descontar la cuota.',
                     'eligibility':'Las reglas de elegibilidad son heurísticas académicas.',
                     'currency':'USD'}
        if customer_id:
            view='profile'
            query='SELECT user_id,record_date,features FROM serving.financial_profiles WHERE user_id=%s ORDER BY record_date DESC LIMIT 100'
            params=[customer_id]
        else:
            view='portfolio'
            query=('SELECT COUNT(*) AS profiles,COUNT(DISTINCT user_id) AS users,'
                   'AVG(monthly_income_usd) AS mean_income_usd,AVG(monthly_expenses_usd) AS mean_expenses_usd,'
                   'AVG(monthly_income_usd-monthly_expenses_usd) AS mean_cash_before_loan,'
                   "AVG(COALESCE((features->>'monthly_emi_usd')::numeric,0)) AS mean_loan_payment_usd,"
                   'AVG(savings_capacity) AS mean_savings_capacity,AVG(credit_score) AS mean_credit_score '
                   'FROM serving.financial_profiles')
    elif dataset=='dataset4':
        view='risk_model'
        query='SELECT metrics FROM serving.credit_default_models'
        definitions={'source':'UCI Default of Credit Card Clients, Taiwán 2005, CC BY 4.0',
                     'purpose':'Modelo supervisado de impago del siguiente mes; validación fuera de muestra.'}
    elif dataset=='personal':
        selected=customer_id or os.environ['DEMO_PROFILE_ID']
        if view=='monthly':
            query='SELECT * FROM serving.personal_monthly_cashflow WHERE profile_id=%s ORDER BY period_month'
        elif view in ['categories','category_monthly']:
            query='SELECT * FROM serving.personal_category_spending WHERE profile_id=%s ORDER BY period_month,category'
        elif view=='cards':
            query='SELECT * FROM serving.personal_card_history WHERE profile_id=%s ORDER BY period_month'
        else:
            view='personal_profile'
            query='SELECT profile_id,payload FROM serving.personal_profiles WHERE profile_id=%s'
        params=[selected]
        definitions={'identity':'Persona ficticia con identidad común, distinta de los portafolios originales.',
                     'expenses':'Los gastos personales incluyen la cuota del préstamo. Se descuenta una sola vez.'}
    else:
        raise ValueError('Dataset o vista no disponible')
    with db_connect(reader=True) as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        cursor.execute("SET LOCAL statement_timeout = '8000'")
        if source_run_id is None:
            cursor.execute('SELECT run_id,published_at FROM serving.publication')
        else:
            cursor.execute('SELECT run_id,published_at FROM serving.history_publications WHERE run_id=%s',(source_run_id,))
        publication=cursor.fetchone()
        if not publication:
            raise ValueError('No hay una publicación disponible. Revisa la tarea publish.')
        publication['published_at']=publication['published_at'].astimezone(ZoneInfo(os.getenv('APP_TIMEZONE','America/Mazatlan')))
        tables=['monthly_cashflow','category_spending','credit_cards','credit_card_anomalies','financial_profiles','credit_default_models',
                'personal_profiles','personal_monthly_cashflow','personal_category_spending','personal_card_history']
        for table in tables:
            query=query.replace('FROM serving.'+table,'FROM (SELECT * FROM serving.history_'+table+' WHERE source_run_id=%s) scoped')
        cursor.execute(query,[publication['run_id'],*params])
        return {'publication':dict(publication),'dataset':dataset,'view':view,'definitions':definitions,
                'rows':[dict(row) for row in cursor.fetchall()]}


def query_simulations(profile_id=None, simulation_run_id=None, limit=5):
    query='SELECT * FROM simulation.runs WHERE true'
    params=[]
    if profile_id:
        query+=' AND profile_id=%s'
        params.append(profile_id)
    if simulation_run_id:
        query+=' AND simulation_run_id=%s'
        params.append(simulation_run_id)
    query+=' ORDER BY created_at DESC LIMIT %s'
    params.append(min(max(int(limit),1),10))
    with db_connect(reader=True) as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query,params)
        runs=[dict(row) for row in cursor.fetchall()]
        for run in runs:
            horizon=run['configuration']['months']
            cursor.execute('SELECT scenario,month,period_month,p05,p50,p95,probability_negative,probability_ever_negative '
                           'FROM simulation.results WHERE simulation_run_id=%s AND month=%s ORDER BY scenario',
                           (run['simulation_run_id'],horizon))
            run['final_results']=[dict(row) for row in cursor.fetchall()]
    return {'simulations':runs}


def query_publications():
    with db_connect(reader=True) as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute('SELECT run_id,published_at,manifest FROM serving.history_publications ORDER BY published_at DESC LIMIT 10')
        return {'publications':[dict(row) for row in cursor.fetchall()]}


TOOLS = [
    {'type':'function','function':{'name':'consultar_gold',
      'description':'Consulta las tablas Gold publicadas. Usa monthly o categories para periodos de Dataset1. Dataset2 permite cluster o cust_id, Dataset3 perfil o user_id. Son fuentes independientes.',
      'parameters':{'type':'object','properties':{
          'dataset':{'type':'string','enum':['dataset1','dataset2','dataset3','dataset4','personal']},
          'view':{'type':'string','enum':['summary','monthly','categories','category_monthly','segments','anomalies','profile','cards']},
          'customer_id':{'type':'string','description':'Identificador exacto de la fuente, solo si lo proporcionó el usuario'},
          'source_run_id':{'type':'string','description':'Identificador exacto de publicación, solo para una fotografía histórica solicitada'},
          'date_from':{'type':'string','description':'Primer mes incluido YYYY-MM-01'},
          'date_to':{'type':'string','description':'Último mes incluido YYYY-MM-01'}},'required':['dataset','view'],'additionalProperties':False}}},
    {'type':'function','function':{'name':'simular_flujo',
      'description':'Ejecuta Monte Carlo para Dataset1 desde Gold publicado. Solo cuando el usuario pide una simulación. Solicita primero su ahorro inicial si no lo indicó. Usa los escenarios base por defecto.',
      'parameters':{'type':'object','properties':{
          'initial_savings':{'type':'number','minimum':0},
          'months':{'type':'integer','minimum':1,'maximum':60},
          'simulations':{'type':'integer','minimum':100,'maximum':50000},
          'seed':{'type':'integer','minimum':0,'maximum':4294967295},
          'scenarios':{'type':'array','minItems':1,'maxItems':5,'items':{'type':'object','properties':{
              'name':{'type':'string'},'income_change_pct':{'type':'number','description':'Cambio en puntos porcentuales. Para +10% envía 10. Para -10% envía -10. No envíes fracciones.'},
              'expense_change_pct':{'type':'number','description':'Cambio en puntos porcentuales. Para gastos +10% envía 10, no 0.10. Para -10% envía -10.'},'extra_monthly_expense':{'type':'number'}},
              'required':['name','income_change_pct','expense_change_pct','extra_monthly_expense'],'additionalProperties':False}}},
          'required':['initial_savings'],'additionalProperties':False}}},
]


TOOLS.extend([
    {'type':'function','function':{'name':'consultar_simulaciones','description':'Recupera escenarios ya publicados y sus resultados finales.',
     'parameters':{'type':'object','properties':{'profile_id':{'type':'string'},'simulation_run_id':{'type':'string'},
                    'limit':{'type':'integer','minimum':1,'maximum':10}},'additionalProperties':False}}},
    {'type':'function','function':{'name':'consultar_publicaciones','description':'Enumera fotografías Gold, productos y validaciones disponibles.',
     'parameters':{'type':'object','properties':{},'additionalProperties':False}}},
])


class AgenteFinancieroGroq:
    def __init__(self):
        key = os.getenv('GROQ_API_KEY')
        self.client = Groq(api_key=key, timeout=60, max_retries=2) if key else None
        self.model = os.getenv('GROQ_MODEL','openai/gpt-oss-120b')
        self.catalog, self.catalog_warning = load_data_catalog()
        self.last_evidence = []

    def estado(self):
        return {'groq_configured':self.client is not None, 'catalog_loaded':bool(self.catalog),
                'catalog_warning':self.catalog_warning, 'model':self.model}

    def generar_respuesta(self, prompt_usuario, history=None):
        if not prompt_usuario or not prompt_usuario.strip():
            return 'Escribe una pregunta para poder ayudarte.'
        self.last_evidence = []
        try:
            overview = query_gold('dataset1')
            monthly = query_gold('dataset1','monthly',source_run_id=overview['publication']['run_id'])
            monthly['rows'] = monthly['rows'][-12:]
            monthly['coverage_note'] = 'Muestra de últimos 12 meses. Consultar monthly con fechas para otros periodos.'
            self.last_evidence = [overview, monthly]
        except Exception as exc:
            return 'No pude leer la publicación Gold. ' + redact(str(exc))
        if self.client is None:
            return ('Falta configurar GROQ_API_KEY en .env. La conexión Gold funciona y contiene '
                    + str(overview['rows'][0]['months']) + ' meses publicados.')
        system = '''Eres el asistente del Gemelo Digital Financiero Personal, un prototipo académico.
Responde en español con lenguaje claro y directo. Usa exclusivamente las cifras de las herramientas o
la evidencia Gold proporcionada. Cada respuesta cuantitativa debe indicar el periodo y la fuente Dataset.
Si faltan datos, dilo y pide el dato concreto. Puedes contestar preguntas financieras sobre estas fuentes.
Dataset1 son transacciones sin identificador de persona ni moneda confirmada. Es el perfil demostrativo
transaccional. Dataset2 son tarjetas identificadas por cust_id. Dataset3 son perfiles sintéticos en USD
identificados por user_id. No relaciones a sus personas, saldos o monedas. No atribuyas el portafolio
agregado a una persona. No uses la suma histórica del flujo como saldo inicial real.
Llama consultar_gold para categorías, tarjetas y perfiles específicos. No inventes consultas ni resultados.
El catálogo contiene metadatos históricos, mientras las herramientas contienen resultados actuales.
Puedes simular ingresos/gastos de Dataset1 por Monte Carlo si el usuario lo pide. Pide initial_savings
si aún no fue indicado. Ofrece 0 como supuesto explícito y espera su elección. Puede ajustar semilla,
número de simulaciones, horizonte y hasta 5 escenarios por conversación. No reutilices saldos de Dataset3.
Los parámetros income_change_pct y expense_change_pct usan puntos porcentuales. Para 10% envía 10.
Usa los escenarios por defecto omitiendo scenarios si se solicitan Base, Gastos +10% e Ingresos -10%.
Al explicar el cambio usa la configuración efectiva devuelta por la herramienta.
La simulación remuestrea pares mensuales completos, sin estacionalidad, inflación, intereses ni tendencia.
P05/P50/P95 son percentiles de trayectorias simuladas, no intervalos de confianza de un estimador.
Dataset3 interpreta la capacidad como ingreso menos gastos menos EMI. Usa mean_cash_before_loan para distinguir el margen antes de la cuota y mean_loan_payment_usd para explicar la diferencia.
La persona ficticia usa gastos que ya incluyen la cuota y se consulta como personal.
Dataset4 expone el modelo supervisado de impago y sus métricas, no una decisión de crédito.
Consulta simulaciones y publicaciones existentes con sus herramientas cuando se soliciten.
La probabilidad de saldo negativo depende de los supuestos. No es probabilidad de impago crediticio.
Interpreta los resultados con sus supuestos, moneda sin confirmar y límites de cobertura histórica.
Usa las mismas unidades de la fuente. No presentes anomalías como fraude confirmado ni heurísticas
como aprobaciones crediticias. Identifica la fuente y el periodo. Evita nombres de vistas internas y detalles de infraestructura salvo que se soliciten.
'''
        metadata = {key:{'name':value.get('name'),'columns':[column['silver_name'] for column in value.get('columns',[])],
                         'limitations':value.get('limitations',[])[:2]}
                    for key,value in self.catalog.get('datasets',{}).items()}
        system += '\nMETADATOS\n' + json.dumps(metadata, ensure_ascii=False, default=str)
        system += '\nEVIDENCIA GOLD ACTUAL\n' + json.dumps(self.last_evidence, ensure_ascii=False, default=str)
        messages = [{'role':'system','content':system}]
        recent = []
        characters = 0
        for message in reversed((history or [])[-12:]):
            content = str(message.get('content',''))
            if characters + len(content) > 4000:
                break
            if message.get('role') in ('user','assistant'):
                recent.append({'role':message['role'],'content':content})
                characters += len(content)
        messages.extend(reversed(recent))
        messages.append({'role':'user','content':prompt_usuario.strip()[:6000]})
        simulation_done = False
        try:
            for _ in range(5):
                completion = self.client.chat.completions.create(
                    model=self.model, messages=messages, tools=TOOLS, tool_choice='auto',
                    temperature=0.1, max_tokens=1400)
                message = completion.choices[0].message
                if not message.tool_calls:
                    return message.content.strip() if message.content else 'No recibí una respuesta del modelo.'
                messages.append(message.model_dump(exclude_none=True))
                for call in message.tool_calls:
                    try:
                        arguments = json.loads(call.function.arguments)
                        if call.function.name == 'consultar_gold':
                            arguments.setdefault('source_run_id',overview['publication']['run_id'])
                            output = query_gold(**arguments)
                        elif call.function.name == 'consultar_simulaciones':
                            output = query_simulations(**arguments)
                        elif call.function.name == 'consultar_publicaciones':
                            output = query_publications()
                        elif call.function.name == 'simular_flujo' and not simulation_done:
                            from src.analytics.run_monte_carlo import run_simulation
                            output = run_simulation(source_run_id=overview['publication']['run_id'],configuration=MonteCarloConfig(**arguments))
                            final_month = output['metadata']['configuration']['months']
                            output['results'] = [row for row in output['results'] if row['month'] == final_month]
                            output['results_note'] = 'Se muestran resultados finales. La serie mensual completa queda publicada.'
                            simulation_done = True
                        else:
                            raise ValueError('Herramienta no disponible o simulación ya ejecutada en este turno')
                        self.last_evidence.append(output)
                    except Exception as exc:
                        output = {'error':redact(str(exc))}
                    messages.append({'role':'tool','tool_call_id':call.id,
                                     'content':json.dumps(output, ensure_ascii=False, default=str)})
            return 'La consulta requiere demasiados pasos. Indica el dataset y periodo que quieres analizar.'
        except Exception as exc:
            return 'No pude completar la respuesta del modelo. ' + redact(str(exc))

    def __call__(self, prompt_usuario):
        return self.generar_respuesta(prompt_usuario)


agente_financiero = AgenteFinancieroGroq()
