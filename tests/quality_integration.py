"""Ejercita las tres fuentes con errores controlados en corridas aisladas."""
import csv
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

from src.common.config import ROOT, db_connect


def check():
    run_id='quality_' + uuid.uuid4().hex
    with tempfile.TemporaryDirectory(prefix='gemelo-dirty-') as directory:
        for index in [1,2,3]:
            source=ROOT/'data/test'/f'Dataset{index}_dirty.csv'
            shutil.copy2(source,Path(directory)/f'Dataset{index}.csv')
            env={**os.environ,'SOURCE_DATA_DIR':directory}
            for job in [f'bronze{index}',f'silver{index}']:
                subprocess.run([sys.executable,'-m','src.pipeline','--job',job,'--run-id',run_id],env=env,check=True)
            with source.open() as handle:
                expected=sum(1 for row in csv.DictReader(handle))
            with db_connect() as conn, conn.cursor() as cursor:
                cursor.execute('SELECT metrics FROM ops.job_runs WHERE run_id=%s AND task_id=%s', (run_id,f'silver{index}'))
                metrics=cursor.fetchone()[0]
            assert metrics['records_read']==expected
            assert expected==metrics['duplicates_removed']+metrics['records_quarantined']+metrics['records_written']
            assert metrics['records_quarantined']>0
            print('CONCILIACION_OK',index,metrics['records_read'],metrics['duplicates_removed'],
                  metrics['records_quarantined'],metrics['records_written'])
    print('CALIDAD_INTEGRAL_OK',run_id)


if __name__=='__main__':
    check()
