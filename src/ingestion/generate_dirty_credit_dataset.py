import random
from pathlib import Path

import pandas as pd


INPUT_FILE = Path("data/raw/Dataset2.csv")
OUTPUT_FILE = Path("data/test/Dataset2_dirty.csv")

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

    # 1. Valores nulos en CREDIT_LIMIT

    indexes = random.sample(
        list(dirty_df.index),
        min(20, len(dirty_df))
    )

    dirty_df.loc[indexes, "CREDIT_LIMIT"] = None

    # 2. Valores nulos en MINIMUM_PAYMENTS

    indexes = random.sample(
        list(dirty_df.index),
        min(20, len(dirty_df))
    )

    dirty_df.loc[indexes, "MINIMUM_PAYMENTS"] = None

    # 3. Valores negativos inválidos

    indexes = random.sample(
        list(dirty_df.index),
        min(10, len(dirty_df))
    )

    for index in indexes:

        value = dirty_df.loc[index, "BALANCE"]

        if pd.notna(value):
            dirty_df.loc[index, "BALANCE"] = -abs(float(value))

    # 4. Valores negativos en PAYMENTS

    indexes = random.sample(
        list(dirty_df.index),
        min(10, len(dirty_df))
    )

    for index in indexes:

        value = dirty_df.loc[index, "PAYMENTS"]

        if pd.notna(value):
            dirty_df.loc[index, "PAYMENTS"] = -abs(float(value))

    # 5. Formatos monetarios inconsistentes

    indexes = random.sample(
        list(dirty_df.index),
        min(20, len(dirty_df))
    )

    dirty_df["PURCHASES"] = dirty_df["PURCHASES"].astype("object")

    for index in indexes:

        value = dirty_df.loc[index, "PURCHASES"]

        if pd.notna(value):

            dirty_df.loc[index, "PURCHASES"] = (
                f" ${float(value):.2f} "
            )

    # 6. BALANCE_FREQUENCY fuera de rango

    indexes = random.sample(
        list(dirty_df.index),
        min(10, len(dirty_df))
    )

    dirty_df.loc[
        indexes,
        "BALANCE_FREQUENCY"
    ] = 1.5

    # 7. PRC_FULL_PAYMENT fuera de rango

    indexes = random.sample(
        list(dirty_df.index),
        min(10, len(dirty_df))
    )

    dirty_df.loc[
        indexes,
        "PRC_FULL_PAYMENT"
    ] = 1.25

    # 8. TENURE inválido

    indexes = random.sample(
        list(dirty_df.index),
        min(10, len(dirty_df))
    )

    for i, index in enumerate(indexes):

        if i % 2 == 0:
            dirty_df.loc[index, "TENURE"] = 0
        else:
            dirty_df.loc[index, "TENURE"] = 100

    # 9. Duplicados

    duplicate_count = min(25, len(dirty_df))

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

    # 10. Guardar

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
    print("DATASET CREDITICIO CON ERRORES")
    print("========================================")
    print(f"Archivo: {OUTPUT_FILE}")
    print(f"Registros finales: {len(dirty_df)}")
    print(f"Columnas: {len(dirty_df.columns)}")
    print()
    print("Errores introducidos:")
    print("- Valores nulos")
    print("- Saldos negativos")
    print("- Pagos negativos")
    print("- Formatos monetarios inconsistentes")
    print("- Frecuencias fuera de rango")
    print("- Porcentaje de pago completo fuera de rango")
    print("- TENURE inválido")
    print("- Registros duplicados")


if __name__ == "__main__":
    generate_dirty_dataset()