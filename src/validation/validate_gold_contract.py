"""Validador genérico de contratos JSON para DataFrames PySpark de la capa Gold."""

import json
from pathlib import Path

from pyspark.sql import functions as F


_TYPE_ALIASES = {
    "string": {"string"},
    "integer": {"int", "integer"},
    "long": {"bigint", "long"},
    "double": {"double"},
    "boolean": {"boolean"},
    "date": {"date"},
    "timestamp": {"timestamp"},
    "decimal(18,2)": {"decimal(18,2)"},
    "decimal(12,6)": {"decimal(12,6)"},
    "decimal(12,1)": {"decimal(12,1)"},
    "decimal(9,6)": {"decimal(9,6)"},
    "decimal(9,4)": {"decimal(9,4)"},
}


def validate_gold_contract(df, contract_path, raise_on_error=True):
    """Valida columnas, tipos, nulos, llaves, dominios y checks del contrato.

    El contrato JSON es la fuente de verdad. Los conteos de nulidad, dominios,
    rangos y reglas por fila se ejecutan en una sola agregación de Spark.
    La unicidad de la llave primaria requiere una agregación adicional.

    Args:
        df: DataFrame PySpark que se desea validar.
        contract_path: Ruta al contrato JSON.
        raise_on_error: Si es True, lanza ValueError cuando hay incumplimientos.

    Returns:
        dict: Resultado con esquema, estado, filas revisadas, errores y warnings.
    """
    contract_path = Path(contract_path)
    with contract_path.open("r", encoding="utf-8") as contract_file:
        contract = json.load(contract_file)

    errors = []
    warnings = []
    pending_checks = []

    # En los contratos Gold, "columns" es una LISTA de objetos con name/type/etc.
    contract_columns = contract.get("columns", [])
    if not isinstance(contract_columns, list):
        raise ValueError(
            "Formato de contrato inválido: 'columns' debe ser una lista de objetos."
        )

    expected_columns = {item["name"]: item for item in contract_columns}
    actual_fields = {field.name: field for field in df.schema.fields}

    missing_columns = sorted(set(expected_columns) - set(actual_fields))
    unexpected_columns = sorted(set(actual_fields) - set(expected_columns))

    if missing_columns:
        errors.append(f"Faltan columnas obligatorias: {missing_columns}")
    if unexpected_columns:
        warnings.append(
            f"Columnas adicionales no declaradas: {unexpected_columns}"
        )

    def add_count_check(condition, message):
        """Agrega una condición para contabilizarla en una única acción Spark."""
        alias = f"quality_check_{len(pending_checks)}"
        pending_checks.append((alias, condition, message))

    # 1. Validación de tipos y reglas individuales por columna.
    for name, spec in expected_columns.items():
        if name not in actual_fields:
            continue

        # La nulabilidad se valida aunque el tipo no coincida.
        if not spec.get("nullable", True):
            add_count_check(
                F.col(name).isNull(),
                lambda count_value, column=name: (
                    f"Columna '{column}': {count_value} valores nulos; "
                    "la columna no permite NULL."
                ),
            )

        actual_type = actual_fields[name].dataType.simpleString().lower()
        expected_type = spec["type"].lower()
        allowed_types = _TYPE_ALIASES.get(expected_type, {expected_type})

        if actual_type not in allowed_types:
            errors.append(
                f"{name}: tipo esperado {expected_type}, recibido {actual_type}"
            )
            # Evitar comparaciones de rango/dominio con un tipo incorrecto.
            continue

        checks = spec.get("checks", {})

        if "allowed_values" in checks:
            allowed_values = checks["allowed_values"]
            add_count_check(
                F.col(name).isNotNull()
                & (~F.col(name).isin(allowed_values)),
                lambda count_value, column=name: (
                    f"{column}: {count_value} valores fuera de allowed_values"
                ),
            )

        if "min" in checks:
            minimum = checks["min"]
            add_count_check(
                F.col(name).isNotNull()
                & (F.col(name) < F.lit(minimum)),
                lambda count_value, column=name, bound=minimum: (
                    f"{column}: {count_value} valores menores que {bound}"
                ),
            )

        if "max" in checks:
            maximum = checks["max"]
            add_count_check(
                F.col(name).isNotNull()
                & (F.col(name) > F.lit(maximum)),
                lambda count_value, column=name, bound=maximum: (
                    f"{column}: {count_value} valores mayores que {bound}"
                ),
            )

        if "min_exclusive" in checks:
            minimum = checks["min_exclusive"]
            add_count_check(
                F.col(name).isNotNull()
                & (F.col(name) <= F.lit(minimum)),
                lambda count_value, column=name, bound=minimum: (
                    f"{column}: {count_value} valores deben ser mayores que {bound}"
                ),
            )

    # 2. Llave primaria: componentes no nulos y combinaciones únicas.
    primary_key = contract.get("primary_key", []) or []
    missing_pk = [name for name in primary_key if name not in actual_fields]

    if missing_pk:
        errors.append(f"Faltan columnas de llave primaria: {missing_pk}")
    elif primary_key:
        null_pk_condition = None
        for name in primary_key:
            condition = F.col(name).isNull()
            null_pk_condition = (
                condition
                if null_pk_condition is None
                else (null_pk_condition | condition)
            )

        add_count_check(
            null_pk_condition,
            lambda count_value: (
                f"Llave primaria: {count_value} filas con componentes NULL"
            ),
        )

    # 3. Reglas condicionales (p. ej. has_loan=true exige campos del préstamo).
    for rule in contract.get("conditional_rules", []):
        when_spec = rule["when"]
        condition_column = when_spec["column"]

        if condition_column not in actual_fields:
            errors.append(
                f"Regla condicional: falta columna {condition_column}"
            )
            continue

        condition = F.col(condition_column) == F.lit(when_spec["equals"])

        for required_column in rule.get("then_not_null", []):
            if required_column not in actual_fields:
                errors.append(
                    f"Regla condicional: falta columna {required_column}"
                )
                continue

            expected_value = when_spec["equals"]
            add_count_check(
                condition & F.col(required_column).isNull(),
                lambda count_value, cond_col=condition_column,
                cond_value=expected_value, req_col=required_column: (
                    f"Regla condicional: {count_value} filas con "
                    f"{cond_col}={cond_value} y {req_col}=NULL"
                ),
            )

    # 4. Checks aritméticos declarativos; no se ejecuta SQL arbitrario del JSON.
    for rule in contract.get("row_checks", []):
        rule_type = rule["type"]
        target = rule["target"]
        left_name = rule.get("left")
        right_name = rule.get("right")
        referenced = [target, left_name, right_name]

        missing_references = [
            name for name in referenced
            if not name or name not in actual_fields
        ]
        if missing_references:
            errors.append(
                f"Row check {rule['name']}: faltan columnas {missing_references}"
            )
            continue

        left = F.col(left_name)
        right = F.col(right_name)

        if rule_type == "difference_equals":
            expected = left - right
        elif rule_type == "sum_equals":
            expected = left + right
        else:
            errors.append(
                f"Row check {rule['name']}: tipo no soportado {rule_type}"
            )
            continue

        tolerance = float(rule.get("tolerance", 0.0))
        add_count_check(
            F.col(target).isNotNull()
            & left.isNotNull()
            & right.isNotNull()
            & (
                F.abs(
                    F.col(target).cast("double") - expected.cast("double")
                ) > F.lit(tolerance)
            ),
            lambda count_value, rule_name=rule["name"]: (
                f"Row check {rule_name}: {count_value} filas inconsistentes"
            ),
        )

    # 5. Una sola acción de Spark para filas y todos los conteos anteriores.
    aggregate_expressions = [
        F.count(F.lit(1)).alias("_rows_checked")
    ]
    aggregate_expressions.extend(
        F.coalesce(
            F.sum(F.when(condition, 1).otherwise(0)),
            F.lit(0),
        ).cast("long").alias(alias)
        for alias, condition, _ in pending_checks
    )

    aggregate_result = df.agg(*aggregate_expressions).first().asDict()
    rows_checked = int(aggregate_result.get("_rows_checked", 0) or 0)

    for alias, _condition, message_builder in pending_checks:
        count_value = int(aggregate_result.get(alias, 0) or 0)
        if count_value > 0:
            errors.append(message_builder(count_value))

    # 6. Unicidad de llave primaria (una acción adicional de Spark).
    if primary_key and not missing_pk:
        duplicate_count = (
            df.groupBy(*primary_key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicate_count:
            errors.append(
                f"Llave primaria {primary_key}: "
                f"{duplicate_count} combinaciones duplicadas"
            )

    result = {
        "schema_name": contract.get("schema_name"),
        "contract_version": contract.get("contract_version"),
        "status": "PASSED" if not errors else "FAILED",
        "rows_checked": rows_checked,
        "errors": errors,
        "warnings": warnings,
    }

    if errors and raise_on_error:
        raise ValueError(
            f"Contrato Gold {contract.get('schema_name')} incumplido: "
            + " | ".join(errors)
        )

    return result
