"""Entrada única de tareas con métricas y logs consistentes."""
import argparse
import logging
import os
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from src.common.config import ROOT, redact, safe_run_id
from src.common.observability import tracked_job


class Tee:
    def __init__(self, console, logfile):
        self.console, self.logfile = console, logfile

    def write(self, text):
        safe = redact(text)
        self.console.write(safe)
        if not self.logfile.closed:
            self.logfile.write(safe)
            self.logfile.flush()
        return len(text)

    def flush(self):
        self.console.flush()
        if not self.logfile.closed:
            self.logfile.flush()


def execute(job, run_id):
    if job == 'risk_ingestion':
        from src.ingestion.ingest_dataset_pyspark import ingest_dataset
        return ingest_dataset(str(ROOT/'data/raw/CreditDefault.csv'),'CreditDefault',run_id)
    if job == 'risk_features':
        from src.transformation.credit_default_features import prepare_credit_default
        return prepare_credit_default(run_id)
    if job == 'risk_model':
        from src.ml.train_credit_default import train
        return train(run_id)
    if job == 'demo_ingestion':
        from src.ingestion.ingest_personal_demo import ingest_demo
        return ingest_demo(run_id)
    if job == 'personal_profile':
        from src.transformation.personal_profile import build_personal
        return build_personal(run_id)
    if job == 'personal_simulation':
        from src.analytics.run_monte_carlo import run_simulation
        result=run_simulation(source_run_id=run_id,profile_id=os.environ['DEMO_PROFILE_ID'],
                              simulation_run_id='personal_' + __import__('hashlib').sha256(run_id.encode()).hexdigest()[:32])
        print('Escenarios personales publicados',result['simulation_run_id'])
        return
    if job.startswith('bronze'):
        from src.ingestion.ingest_dataset_pyspark import ingest_dataset
        dataset = int(job[-1])
        source_dir = os.getenv('SOURCE_DATA_DIR', str(ROOT/'data/raw'))
        return ingest_dataset(str(Path(source_dir)/f'Dataset{dataset}.csv'), f'Dataset{dataset}', run_id)
    if job.startswith('silver'):
        from src.transformation.silver_pipeline import process_silver
        return process_silver(int(job[-1]), run_id)
    if job.startswith('gold'):
        from importlib import import_module
        module = import_module(f'src.transformation.silver_to_gold_dataset{job[-1]}_pyspark')
        function = {'gold1':'process_gold_dataset1','gold2':'process_gold_credit_card','gold3':'process_gold_financial_profile'}[job]
        return getattr(module,function)(run_id)
    if job == 'anomalies':
        from src.ml.train_isolation_forest_dataset2 import run
        return run(argparse.Namespace(run_id=run_id,source_run_id=run_id,input_path=None,contamination=.02,
                                      seed=42,n_estimators=200,max_pandas_rows=100000,stability_test=True))
    if job == 'publish':
        from src.analytics.publish_gold import publish
        return publish(run_id)
    if job == 'monte_carlo':
        from src.analytics.run_monte_carlo import run_simulation
        result = run_simulation(source_run_id=run_id,simulation_run_id='dag_mc_' + __import__('hashlib').sha256(run_id.encode()).hexdigest()[:32])
        print('Simulación publicada', result['simulation_run_id'])
        return
    raise ValueError(f'Job desconocido {job}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--job',required=True,choices=['bronze1','bronze2','bronze3','silver1','silver2','silver3','gold1','gold2','gold3','anomalies','publish','monte_carlo','risk_ingestion','risk_features','risk_model','demo_ingestion','personal_profile','personal_simulation'])
    parser.add_argument('--run-id',required=True)
    args = parser.parse_args()
    layer = args.job.rstrip('123')
    dataset = 'Dataset' + args.job[-1] if args.job[-1] in '123' else {'anomalies':'Dataset2','monte_carlo':'Dataset1','publish':'all','risk_ingestion':'CreditDefault','risk_features':'CreditDefault','risk_model':'CreditDefault','demo_ingestion':'Personal','personal_profile':'Personal','personal_simulation':'Personal'}[args.job]
    log_dir = ROOT/'logs'/'pipeline'/safe_run_id(args.run_id)
    log_dir.mkdir(parents=True,exist_ok=True)
    log_path = log_dir/(args.job + '_' + os.getenv('PIPELINE_ATTEMPT','1') + '.log')
    os.environ['PIPELINE_TASK_LOG'] = str(log_path)
    with log_path.open('w') as logfile, redirect_stdout(Tee(sys.stdout,logfile)), redirect_stderr(Tee(sys.stderr,logfile)):
        logging.basicConfig(level=logging.INFO,stream=sys.stderr,force=True)
        with tracked_job(args.run_id,args.job,dataset,layer):
            execute(args.job,args.run_id)


if __name__ == '__main__':
    main()
