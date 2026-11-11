"""Ingesta trazable de las fuentes de la persona ficticia, con identidad común."""
import csv
import hashlib
import json

from minio import Minio

from src.common.config import ROOT, lake_path, required
from src.common.observability import record_metrics
from src.ingestion.ingest_dataset_pyspark import ingest_dataset


def ingest_demo(run_id):
    folder=ROOT/'data/demo'
    ingest_dataset(str(folder/'transactions.csv'),'PersonalTransactions',run_id)
    ingest_dataset(str(folder/'cards.csv'),'PersonalCards',run_id)
    profile=json.loads((folder/'profile.json').read_text())
    endpoint=required('MINIO_ENDPOINT')
    client=Minio(endpoint.split('://',1)[-1],access_key=required('MINIO_ROOT_USER'),
                 secret_key=required('MINIO_ROOT_PASSWORD'),secure=endpoint.startswith('https://'))
    prefix=lake_path('bronze','PersonalProfile',run_id).removeprefix('s3a://bronze/')
    digest=hashlib.sha256((folder/'profile.json').read_bytes()).hexdigest()
    client.fput_object('bronze',prefix+'/profile.json',str(folder/'profile.json'),content_type='application/json')
    sources={}
    for filename in ['transactions.csv','cards.csv']:
        with (folder/filename).open(newline='') as stream:
            sources[filename]={'records':sum(1 for _ in csv.DictReader(stream)),
                               'sha256':hashlib.sha256((folder/filename).read_bytes()).hexdigest()}
    total=sum(item['records'] for item in sources.values())
    record_metrics(records_read=total,records_written=total,profile_id=profile['profile_id'],
                   profile_sha256=digest,source_kind='synthetic_demo',sources=sources)
    print('Fuentes de la persona ficticia publicadas',profile['profile_id'])
