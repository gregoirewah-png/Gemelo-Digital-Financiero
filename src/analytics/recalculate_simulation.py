"""Configuración JSON del DAG independiente de simulación."""
import json
import os
import sys
import logging
from contextlib import redirect_stdout, redirect_stderr
from src.common.config import ROOT, safe_run_id
from src.pipeline import Tee

from src.analytics.run_monte_carlo import run_simulation
from src.common.observability import tracked_job
from src.ml.monte_carlo import MonteCarloConfig

if __name__ == '__main__':
    parameters = json.loads(os.environ.get('SIMULATION_CONFIG','{}'))
    source = parameters.pop('source_run_id',None)
    profile_id = parameters.pop('profile_id',None)
    if profile_id and 'initial_savings' not in parameters:
        from src.analytics.personal_finance import personal_snapshot
        snapshot=personal_snapshot(profile_id,source)
        source=snapshot['publication']['run_id']
        parameters['initial_savings']=float(snapshot['profile']['available_savings'])
    simulation_id = os.environ['SIMULATION_RUN_ID']
    log_dir = ROOT/'logs'/'pipeline'/safe_run_id(simulation_id)
    log_dir.mkdir(parents=True,exist_ok=True)
    log_path = log_dir/'monte_carlo.log'
    os.environ['PIPELINE_TASK_LOG'] = str(log_path)
    with log_path.open('w') as logfile, redirect_stdout(Tee(sys.stdout,logfile)), redirect_stderr(Tee(sys.stderr,logfile)):
        logging.basicConfig(level=logging.INFO,stream=sys.stderr,force=True)
        with tracked_job(simulation_id,'monte_carlo','Dataset1','monte_carlo'):
            result = run_simulation(source_run_id=source,simulation_run_id=simulation_id,
                                    configuration=MonteCarloConfig(**parameters),profile_id=profile_id)
            print(json.dumps(result['metadata'],ensure_ascii=False))
