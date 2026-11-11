"""Importa reportes Silver históricos al esquema de observabilidad, sin duplicarlos."""
import argparse
import re
from datetime import datetime, timezone
from pathlib import Path

from psycopg2.extras import Json

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import ROOT, db_connect, redact


def parse_and_insert_logs(log_dir=None):
    for path in sorted(Path(log_dir or ROOT/'logs').glob('reporte_dataset*.log')):
        content = path.read_text(errors='replace')
        run_match = re.search(r'Run ID:\s*([^\n]+)',content)
        dataset_match = re.search(r'dataset(\d+)',path.name,re.I)
        if not run_match or not dataset_match:
            continue
        run_id = run_match.group(1).strip()
        dataset = 'Dataset' + dataset_match.group(1)
        patterns = {'records_read':r'Registros leídos.*?:\s*(\d+)',
                    'duplicates_removed':r'Duplicados eliminados:\s*(\d+)',
                    'records_quarantined':r'Registros aislados.*?:\s*(\d+)',
                    'records_written':r'Registros validados.*?:\s*(\d+)',
                    'rules_evaluated':r'Reglas de calidad evaluadas:\s*(\d+)',
                    'rules_failed':r'Reglas fallidas:\s*(\d+)'}
        metrics = {}
        for name,pattern in patterns.items():
            match = re.search(pattern,content)
            if match:
                metrics[name] = int(match.group(1))
        with db_connect() as conn, conn.cursor() as cursor:
            cursor.execute('INSERT INTO ops.job_runs(run_id,task_id,attempt,dataset,layer,status,started_at,metrics,log_content) '
                           "VALUES (%s,%s,1,%s,'silver','SUCCESS',%s,%s,%s) ON CONFLICT DO NOTHING",
                           (run_id,'legacy_silver_'+dataset,dataset,datetime.fromtimestamp(path.stat().st_mtime,tz=timezone.utc),
                            Json(metrics),redact(content)))
        print('Reporte importado',path.name)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--log-dir')
    parse_and_insert_logs(parser.parse_args().log_dir)
