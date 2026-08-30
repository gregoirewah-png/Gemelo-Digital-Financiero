import random
from pathlib import Path

import pandas as pd


INPUT_FILE = Path("data/raw/Dataset3.csv")
OUTPUT_FILE = Path("data/test/Dataset3_dirty.csv")

RANDOM_SEED = 42


def generate_dirty_dataset():

    random.seed(RANDOM_SEED)

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"No se encontró el dataset: {INPUT_FILE}"
        )

    print(f"Leyendo dataset original: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    print(f"Registros originales: {len(df)}")
    print(f"Columnas originales: {len(df.columns)}")

    dirty_df = df.copy()

    # 1. EDADES INVÁLIDAS

    indexes = random.sample(
        list(dirty_df.index),
        min(20, len(dirty_df))
    )

    for i, index in enumerate(indexes):

        if i % 2 == 0:
            dirty_df.loc[index, "age"] = 5
        else:
            dirty_df.loc[index, "age"] = 150

    # 2. INGRESOS NEGATIVOS

    indexes = random.sample(
        list(dirty_df.index),
        min(20, len(dirty_df))
    )

    for index in indexes:

        value = dirty_df.loc[index, "monthly_income_usd"]

        if pd.notna(value):
            dirty_df.loc[index, "monthly_income_usd"] = -abs(
                float(value)
            )

    # 3. GASTOS NEGATIVOS

    indexes = random.sample(
        list(dirty_df.index),
        min(20, len(dirty_df))
    )

    for index in indexes:

        value = dirty_df.loc[index, "monthly_expenses_usd"]

        if pd.notna(value):
            dirty_df.loc[index, "monthly_expenses_usd"] = -abs(
                float(value)
            )

    # 4. AHORROS NEGATIVOS

    indexes = random.sample(
        list(dirty_df.index),
        min(20, len(dirty_df))
    )

    for index in indexes:

        value = dirty_df.loc[index, "savings_usd"]

        if pd.notna(value):
            dirty_df.loc[index, "savings_usd"] = -abs(
                float(value)
            )

    # 5. PRÉSTAMOS NEGATIVOS

    indexes = random.sample(
        list(dirty_df.index),
        min(15, len(dirty_df))
    )

    for index in indexes:

        value = dirty_df.loc[index, "loan_amount_usd"]

        if pd.notna(value):
            dirty_df.loc[index, "loan_amount_usd"] = -abs(
                float(value)
            )

    # 6. PLAZOS DE PRÉSTAMO INVÁLIDOS

    indexes = random.sample(
        list(dirty_df.index),
        min(15, len(dirty_df))
    )

    for i, index in enumerate(indexes):

        if i % 2 == 0:
            dirty_df.loc[index, "loan_term_months"] = 0
        else:
            dirty_df.loc[index, "loan_term_months"] = -12

    # 7. EMI NEGATIVA

    indexes = random.sample(
        list(dirty_df.index),
        min(15, len(dirty_df))
    )

    for index in indexes:

        value = dirty_df.loc[index, "monthly_emi_usd"]

        if pd.notna(value):
            dirty_df.loc[index, "monthly_emi_usd"] = -abs(
                float(value)
            )

    # 8. TASAS DE INTERÉS INVÁLIDAS

    indexes = random.sample(
        list(dirty_df.index),
        min(15, len(dirty_df))
    )

    for i, index in enumerate(indexes):

        if i % 2 == 0:
            dirty_df.loc[index, "loan_interest_rate_pct"] = -5
        else:
            dirty_df.loc[index, "loan_interest_rate_pct"] = 150

    # 9. DTI INVÁLIDO

    indexes = random.sample(
        list(dirty_df.index),
        min(15, len(dirty_df))
    )

    for index in indexes:

        dirty_df.loc[
            index,
            "debt_to_income_ratio"
        ] = -0.5

    # 10. CREDIT SCORE INVÁLIDO

    indexes = random.sample(
        list(dirty_df.index),
        min(15, len(dirty_df))
    )

    for i, index in enumerate(indexes):

        if i % 2 == 0:
            dirty_df.loc[index, "credit_score"] = 100
        else:
            dirty_df.loc[index, "credit_score"] = 1000

    # 11. SAVINGS / INCOME RATIO INVÁLIDO

    indexes = random.sample(
        list(dirty_df.index),
        min(15, len(dirty_df))
    )

    for index in indexes:

        dirty_df.loc[
            index,
            "savings_to_income_ratio"
        ] = -1

    # 12. VALORES NULOS

    null_columns = [
        "monthly_income_usd",
        "monthly_expenses_usd",
        "savings_usd",
        "credit_score"
    ]

    for column in null_columns:

        indexes = random.sample(
            list(dirty_df.index),
            min(15, len(dirty_df))
        )

        dirty_df.loc[
            indexes,
            column
        ] = None

    # 13. DUPLICADOS

    duplicate_count = min(30, len(dirty_df))

    duplicated_rows = dirty_df.sample(
        n=duplicate_count,
        random_state=RANDOM_SEED
    )

    dirty_df = pd.concat(
        [
            dirty_df,
            duplicated_rows
        ],
        ignore_index=True
    )

    # 14. GUARDAR DATASET

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    dirty_df.to_csv(
        OUTPUT_FILE,
        index=False
    )

    print()
    print("========================================")
    print("DATASET 3 CON ERRORES GENERADO")
    print("========================================")
    print(f"Archivo: {OUTPUT_FILE}")
    print(f"Registros finales: {len(dirty_df)}")
    print(f"Columnas: {len(dirty_df.columns)}")
    print()
    print("Errores introducidos:")
    print("- Edades inválidas")
    print("- Ingresos negativos")
    print("- Gastos negativos")
    print("- Ahorros negativos")
    print("- Préstamos negativos")
    print("- Plazos inválidos")
    print("- EMI negativa")
    print("- Tasas de interés inválidas")
    print("- Debt-to-income ratio inválido")
    print("- Credit scores inválidos")
    print("- Savings-to-income ratio inválido")
    print("- Valores nulos")
    print("- Registros duplicados")


if __name__ == "__main__":
    generate_dirty_dataset()