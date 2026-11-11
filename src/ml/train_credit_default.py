"""Entrenamiento versionado, validado y persistido del modelo de impago."""
import io
import json
import os
from pathlib import Path

import joblib
import pandas as pd
from minio import Minio

from src.common.config import ROOT, lake_path, required, safe_run_id
from src.common.observability import record_metrics
from src.common.spark import spark_session, write_lake, read_lake
from src.ml.credit_default_model import train_model


def train(run_id):
    spark=spark_session('Modelo_Impago_UCI')
    try:
        frame=read_lake(spark,lake_path('gold','credit_default_features',run_id))
        if frame.count()>100000:
            raise ValueError('El entrenamiento local admite hasta 100000 registros')
        raw=frame.toPandas()
        bundle,metrics=train_model(raw,seed=int(os.getenv('RISK_SEED','42')))
        if metrics['roc_auc']<float(os.getenv('RISK_MIN_AUC','.65')) or metrics['brier_score']>=metrics['baseline_brier']:
            raise ValueError('El modelo no superó las métricas mínimas frente a la referencia')
        source=json.loads((ROOT/'configs/credit_default_source.json').read_text())
        metrics['source']=source
        folder=ROOT/'models'/'credit_default'/safe_run_id(run_id)
        folder.mkdir(parents=True,exist_ok=True)
        joblib.dump(bundle,folder/'model.joblib')
        (folder/'metrics.json').write_text(json.dumps(metrics,ensure_ascii=False,indent=2))
        model_frame=spark.createDataFrame([(run_id,json.dumps(metrics,ensure_ascii=False))],['model_run_id','metrics_json'])
        write_lake(model_frame,lake_path('gold','credit_default_model',run_id))
        endpoint=required('MINIO_ENDPOINT')
        client=Minio(endpoint.split('://',1)[-1],access_key=required('MINIO_ROOT_USER'),
                     secret_key=required('MINIO_ROOT_PASSWORD'),secure=endpoint.startswith('https://'))
        for filename in ['model.joblib','metrics.json']:
            client.fput_object('models',f'credit_default/{safe_run_id(run_id)}/{filename}',str(folder/filename))
        record_metrics(records_read=len(raw),records_written=1,roc_auc=metrics['roc_auc'],brier_score=metrics['brier_score'])
        print(json.dumps({key:metrics[key] for key in ['rows','train_rows','test_rows','roc_auc','brier_score','baseline_brier']}))
    finally:
        spark.stop()
