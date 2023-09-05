from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os.path import join
from pathlib import Path
from airflow.models import DAG
from datetime import datetime,date,timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator 



with DAG (dag_id='Investimento_api_crypto', start_date=datetime.now(), schedule_interval='0 0 * * 2-6') as dag:

    Spark_extracao_dados = SparkSubmitOperator(
        task_id = 'Extracao_Invest_crypto',
        application = join(str(Path('~/Documents').expanduser()),
                        ('Sprinklr_Airflow/dadosvm/Airflow_Investimento/Scripts/Operators/Raw_Operators_crypto.py')),
        name = 'Extracao_Invest_crypto',
        conn_id='spark_default'
    )

    PostgresOperator = PostgresOperator(
        task_id = 'Insert_Postgres_crypto',
        sql = f"""COPY BTC_Table FROM '{join(Path('~/Documents').expanduser(),f'Sprinklr_Airflow/dadosvm/Airflow_Investimento/datalake/Raw/crypto/Data={(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")}')}/data.csv' CSV HEADER;""",
        postgres_conn_id ='postgres_BTC',
        database = 'BTC_datatable_crypto',
        autocommit=True
    )


Spark_extracao_dados >> PostgresOperator