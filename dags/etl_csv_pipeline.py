from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Ruta base dentro del contenedor
BASE_PATH = "/opt/airflow/dags"

# --------- TASK 1: EXTRACT ---------
def extract():
    data = {
        "nombre": ["Ana", "Luis", "Pedro", "Maria"],
        "edad": [25, 30, 22, 28],
        "ciudad": ["Lima", "Cusco", "Arequipa", "Lima"]
    }
    df = pd.DataFrame(data)

    file_path = os.path.join(BASE_PATH, "data_raw.csv")
    df.to_csv(file_path, index=False)

    print(f"Archivo extraído en: {file_path}")


# --------- TASK 2: TRANSFORM ---------
def transform():
    file_path = os.path.join(BASE_PATH, "data_raw.csv")
    df = pd.read_csv(file_path)

    # Transformación: filtrar mayores de 24 años
    df_filtered = df[df["edad"] > 24]

    output_path = os.path.join(BASE_PATH, "data_clean.csv")
    df_filtered.to_csv(output_path, index=False)

    print("Datos transformados")


# --------- TASK 3: LOAD ---------
def load():
    file_path = os.path.join(BASE_PATH, "data_clean.csv")
    df = pd.read_csv(file_path)

    print("Datos finales:")
    print(df)


# --------- DAG ---------
with DAG(
    dag_id="etl_csv_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "csv"],
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load
    )

    # Orden de ejecución
    t1 >> t2 >> t3
