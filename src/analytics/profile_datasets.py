import pandas as pd
import os


DATASETS = {
    "Dataset1": "data/test/Dataset1_dirty.csv",
    "Dataset2": "data/test/Dataset2_dirty.csv",
    "Dataset3": "data/test/Dataset3_dirty.csv"
}


def profile_dataset(name, path):
    print("\n" + "=" * 70)
    print(f"DATASET: {name}")
    print("=" * 70)

    if not os.path.exists(path):
        print(f"ERROR: No existe el archivo: {path}")
        return

    df = pd.read_csv(path)

    print(f"\nArchivo: {path}")
    print(f"Registros: {len(df)}")
    print(f"Columnas: {len(df.columns)}")

    # 1. Columnas
    print("\n--- COLUMNAS ---")
    print(df.columns.tolist())

    # 2. Tipos de datos
    print("\n--- TIPOS DE DATOS ---")
    print(df.dtypes)

    # 3. Valores nulos
    print("\n--- VALORES NULOS ---")

    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]

    if len(nulls) == 0:
        print("No se encontraron valores nulos.")
    else:
        print(nulls)

    # 4. Duplicados
    print("\n--- DUPLICADOS ---")
    duplicates = df.duplicated().sum()
    print(f"Registros duplicados: {duplicates}")

    # 5. Valores únicos de columnas categóricas
    print("\n--- VALORES CATEGÓRICOS ---")

    for column in df.select_dtypes(include=["object"]).columns:

        unique_count = df[column].nunique(dropna=False)

        if unique_count <= 30:
            print(f"\n{column}:")
            print(df[column].value_counts(dropna=False))

    # 6. Estadísticas numéricas
    print("\n--- ESTADÍSTICAS NUMÉRICAS ---")

    numeric_columns = df.select_dtypes(include=["number"]).columns

    if len(numeric_columns) > 0:
        print(df[numeric_columns].describe().T)

    # 7. Valores negativos
    print("\n--- VALORES NEGATIVOS ---")

    found_negative = False

    for column in numeric_columns:
        count = (df[column] < 0).sum()

        if count > 0:
            found_negative = True
            print(f"{column}: {count}")

    if not found_negative:
        print("No se encontraron valores negativos.")

    # 8. Porcentaje de nulos
    print("\n--- PORCENTAJE DE NULOS ---")

    null_percentage = (df.isnull().mean() * 100)

    print(null_percentage[null_percentage > 0].sort_values(ascending=False))


for name, path in DATASETS.items():
    profile_dataset(name, path)