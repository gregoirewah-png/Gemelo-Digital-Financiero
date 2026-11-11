"""Limpieza declarativa con cuarentena y conciliación de registros por corrida."""
import json

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from src.common.config import ROOT, lake_path
from src.common.observability import record_metrics
from src.common.spark import spark_session, read_lake, write_lake

DATASETS = {1: 'personal_transactions', 2: 'credit_card_balances', 3: 'financial_profile'}


def process_silver(dataset, run_id):
    contract = json.loads((ROOT / 'schemas' / f'dataset{dataset}_schema.json').read_text())
    specs = contract['columns']
    spark = spark_session(f'Silver_Dataset{dataset}')
    raw = clean = evaluated = None
    try:
        schema = StructType([StructField(s['source_name'], StringType(), True) for s in specs.values()]
                            + [StructField('_corrupt_record', StringType(), True)])
        raw = (spark.read.schema(schema).option('header', True).option('mode', 'PERMISSIVE')
               .option('columnNameOfCorruptRecord', '_corrupt_record')
               .csv(lake_path('bronze', f'Dataset{dataset}', run_id) + '/source.csv').cache())
        n_read = raw.count()
        normalized = raw
        for name, spec in specs.items():
            normalized = normalized.withColumnRenamed(spec['source_name'], name)
            value = F.trim(F.col(name))
            value = F.when(value.isin('', 'None', 'null', 'NULL', 'N/A'), F.lit(None)).otherwise(value)
            if spec['type'] == 'date':
                value = F.coalesce(F.to_date(value, 'yyyy-MM-dd'), F.to_date(value, 'M/d/yyyy'),
                                   F.to_date(value, 'yyyy/MM/dd'), F.to_date(value, 'dd-MM-yyyy'))
            elif spec['type'] == 'boolean':
                value = (F.when(F.upper(value).isin('YES', 'TRUE', '1'), F.lit(True))
                         .when(F.upper(value).isin('NO', 'FALSE', '0'), F.lit(False)))
            elif spec['type'] in ('double', 'integer'):
                # El formato de las fuentes es decimal con punto y separador de miles con coma.
                value = F.regexp_replace(value, r'[$,]', '').cast(spec['type'])
            normalized = normalized.withColumn(name, value)
        if dataset == 1:
            normalized = (normalized.withColumn('category', F.initcap('category'))
                          .withColumn('type', F.initcap('type')))
        clean = normalized.dropDuplicates().cache()
        n_unique = clean.count()
        rules = {'estructura_csv': F.col('_corrupt_record').isNotNull()}
        for name, spec in specs.items():
            value = F.col(name)
            if not spec.get('nullable', True):
                rules[f'{name}_requerido'] = value.isNull()
            if spec['type'] == 'string' and not spec.get('nullable', True):
                rules[f'{name}_vacio'] = value == ''
            if spec['type'] == 'double':
                rules[f'{name}_finito'] = value.isNotNull() & (F.isnan(value) | (F.abs(value) == float('inf')))
            if 'allowed_values' in spec:
                rules[f'{name}_dominio'] = value.isNotNull() & ~value.isin(spec['allowed_values'])
            checks = spec.get('checks', {})
            if 'min' in checks:
                rules[f'{name}_min'] = value.isNotNull() & (value < checks['min'])
            if 'min_exclusive' in checks:
                rules[f'{name}_positivo'] = value.isNotNull() & (value <= checks['min_exclusive'])
            if 'max' in checks:
                rules[f'{name}_max'] = value.isNotNull() & (value > checks['max'])
        key = contract.get('primary_key')
        if key:
            clean = clean.withColumn('_key_count', F.count('*').over(Window.partitionBy(*key)))
            rules['llave_duplicada_con_conflicto'] = F.col('_key_count') > 1
        if dataset == 3:
            for name in ['loan_type', 'loan_amount_usd', 'loan_term_months', 'monthly_emi_usd', 'loan_interest_rate_pct']:
                rules[f'{name}_prestamo_requerido'] = F.col('has_loan') & F.col(name).isNull()
            rules['plazo_prestamo_positivo'] = F.col('has_loan') & (F.col('loan_term_months') <= 0)
        reasons = [F.when(condition, F.lit(name)) for name, condition in rules.items()]
        evaluated = (clean.withColumn('quarantine_reason', F.concat_ws(' | ', *reasons)).cache())
        counts = evaluated.agg(*[F.sum(F.when(condition, 1).otherwise(0)).alias(name)
                                for name, condition in rules.items()]).first().asDict()
        invalid = evaluated.filter(F.length('quarantine_reason') > 0)
        valid = evaluated.filter(F.length('quarantine_reason') == 0).select(*specs)
        n_bad = invalid.count()
        n_valid = valid.count()
        n_duplicates = n_read - n_unique
        if n_read != n_duplicates + n_bad + n_valid:
            raise ValueError('No concilian los conteos Bronze/Silver/cuarentena')
        record_metrics(records_read=n_read, records_written=n_valid, records_validated=n_valid,
                       duplicates_removed=n_duplicates, records_quarantined=n_bad,
                       rules_evaluated=len(rules), rules_failed=sum(bool(v) for v in counts.values()),
                       rule_failures=counts, rejected_pct=100 * n_bad / n_read if n_read else None)
        if not n_valid:
            raise ValueError('No hay registros válidos para publicar en Silver')
        write_lake(valid, lake_path('silver', DATASETS[dataset], run_id))
        # Escribir incluso vacía evita dejar residuos de un intento anterior.
        invalid.withColumn('quarantined_at', F.current_timestamp()).coalesce(1).write.mode('overwrite').parquet(
            lake_path('quarantine', f'Dataset{dataset}', run_id))
        print(f'Dataset {dataset}. Leídos {n_read}. Duplicados {n_duplicates}. Cuarentena {n_bad}. Silver {n_valid}.')
        print(json.dumps(counts, ensure_ascii=False))
    finally:
        for frame in [evaluated, clean, raw]:
            if frame is not None:
                frame.unpersist()
        spark.stop()
