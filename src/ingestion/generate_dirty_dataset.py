import random
from pathlib import Path

import pandas as pd


# CONFIGURACIÓN

INPUT_FILE = Path("data/raw/Dataset1.csv")
OUTPUT_FILE = Path("data/test/Dataset1_dirty.csv")

RANDOM_SEED = 42


# GENERACIÓN DEL DATASET CON ERRORES

def generate_dirty_dataset():

    random.seed(RANDOM_SEED)

    # 1. Validar existencia del dataset original

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"No se encontró el dataset original: {INPUT_FILE}"
        )

    print(f"Leyendo dataset original: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    print(f"Registros originales: {len(df)}")
    print(f"Columnas originales: {len(df.columns)}")

    # 2. Crear una copia para no modificar el original

    dirty_df = df.copy()

    # 3. Introducir espacios adicionales

    if len(dirty_df) >= 30:

        description_indexes = random.sample(
            list(dirty_df.index),
            30
        )

        for index in description_indexes:
            value = dirty_df.loc[index, "Transaction Description"]

            if pd.notna(value):
                dirty_df.loc[index, "Transaction Description"] = (
                    f"  {value}  "
                )

    # 4. Introducir inconsistencias en Category

    if len(dirty_df) >= 30:

        category_indexes = random.sample(
            list(dirty_df.index),
            30
        )

        for index in category_indexes:

            value = dirty_df.loc[index, "Category"]

            if pd.notna(value):

                variation = random.choice([
                    str(value).lower(),
                    str(value).upper(),
                    f" {value} ",
                    f"  {value.upper()}  "
                ])

                dirty_df.loc[index, "Category"] = variation

    # 5. Introducir inconsistencias en Type

    if len(dirty_df) >= 20:

        type_indexes = random.sample(
            list(dirty_df.index),
            20
        )

        for index in type_indexes:

            value = dirty_df.loc[index, "Type"]

            if pd.notna(value):

                variation = random.choice([
                    str(value).lower(),
                    str(value).upper(),
                    f" {value} "
                ])

                dirty_df.loc[index, "Type"] = variation

    # 6. Introducir valores nulos en Amount

    if len(dirty_df) >= 10:

        amount_null_indexes = random.sample(
            list(dirty_df.index),
            10
        )

        dirty_df.loc[
            amount_null_indexes,
            "Amount"
        ] = None

    # 7. Introducir formatos monetarios inconsistentes

    if len(dirty_df) >= 20:

        amount_indexes = random.sample(
            list(dirty_df.index),
            20
        )

        for index in amount_indexes:

            value = dirty_df.loc[index, "Amount"]

            if pd.notna(value):

                try:

                    numeric_value = float(value)

                    variation = random.choice([
                        f"${numeric_value:.2f}",
                        f" ${numeric_value:.2f} ",
                        f"{numeric_value:.2f} ",
                        f" {numeric_value:.2f}"
                    ])

                    dirty_df.loc[index, "Amount"] = variation

                except (ValueError, TypeError):
                    pass

    # 8. Introducir formatos inconsistentes de fecha

    if len(dirty_df) >= 30:

        date_indexes = random.sample(
            list(dirty_df.index),
            30
        )

        for index in date_indexes:

            value = dirty_df.loc[index, "Date"]

            if pd.notna(value):

                try:

                    parsed_date = pd.to_datetime(
                        value,
                        errors="coerce"
                    )

                    if pd.notna(parsed_date):

                        variation = random.choice([
                            parsed_date.strftime("%d/%m/%Y"),
                            parsed_date.strftime("%Y/%m/%d"),
                            parsed_date.strftime("%d-%m-%Y")
                        ])

                        dirty_df.loc[
                            index,
                            "Date"
                        ] = variation

                except Exception:
                    pass

    # 9. Introducir duplicados

    duplicate_count = min(15, len(dirty_df))

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

    # 10. Guardar dataset

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
    print("DATASET CON ERRORES GENERADO")
    print("========================================")
    print(f"Archivo: {OUTPUT_FILE}")
    print(f"Registros finales: {len(dirty_df)}")
    print(f"Columnas: {len(dirty_df.columns)}")
    print()
    print("Errores introducidos:")
    print("- Espacios adicionales")
    print("- Categorías inconsistentes")
    print("- Tipos inconsistentes")
    print("- Valores nulos en Amount")
    print("- Formatos monetarios inconsistentes")
    print("- Formatos de fecha inconsistentes")
    print("- Registros duplicados")


# EJECUCIÓN

if __name__ == "__main__":
    generate_dirty_dataset()