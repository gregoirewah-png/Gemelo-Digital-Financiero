"""Silver y Feature Store Gold del dataset público etiquetado de impago."""
import pandas as pd
from pyspark.sql import functions as F

from src.common.config import lake_path
from src.common.observability import record_metrics
from src.common.spark import spark_session, write_lake
from src.ml.credit_default_model import build_credit_features


def prepare_credit_default(run_id):
    spark=spark_session('Silver_Gold_Features_Impago')
    try:
        source=spark.read.option('header',True).csv(lake_path('bronze','CreditDefault',run_id)+'/source.csv')
        if source.count()>100000:
            raise ValueError('El Feature Store local admite hasta 100000 registros')
        names=['ID','X1',*[f'X{i}' for i in range(6,24)],'Y']
        frame=source.select(*names).toPandas()
        frame=frame.apply(pd.to_numeric,errors='raise')
        if frame.ID.isna().any() or frame.ID.duplicated().any() or not frame.Y.isin([0,1]).all():
            raise ValueError('Identificador o etiqueta de impago inválidos')
        features=build_credit_features(frame)
        clean=spark.createDataFrame(frame)
        write_lake(clean,lake_path('silver','credit_default',run_id))
        prepared=pd.concat([frame.reset_index(drop=True),features.reset_index(drop=True)],axis=1)
        gold=spark.createDataFrame(prepared).withColumn('event_month',F.lit('2005-09-01').cast('date'))
        write_lake(gold,lake_path('gold','credit_default_features',run_id))
        record_metrics(records_read=len(frame),records_written=len(frame),features=len(features.columns),
                       label_validation='PASSED',identity_validation='PASSED')
        print('Features Gold preparadas',len(frame),'entidades',len(features.columns),'variables financieras')
    finally:
        spark.stop()
