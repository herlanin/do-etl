import sys

sys.path.append('/usr/local/airflow/')
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from assets.do-etl import xls_to_xlsx, xlsx_extract_sheets_to_csv, diesel_oil_table_transform_partition

# DAG
DAG_NAME_ID = 'do_etl'
DAG_DESCRIPTION = 'This dag extracts caches from a pivot table in a xls file then inserts the data into a table'
OWNER_NAME = 'hugo'
SCHEDULE = 'once'
START_TIME = datetime.today()
default_args = {
    'depends_on_past': False,
    'owner': OWNER_NAME,
    'description': DAG_DESCRIPTION,
    'start_date': START_TIME,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    catchup=False,
    dag_id=DAG_NAME_ID,
    description=DAG_DESCRIPTION,
    schedule_interval=SCHEDULE,
    default_args=default_args
)
xls_file_path = './../data/Vendas_de_Combustiveis_m3.xls'
xlsx_folder_path = './../data/processed/{{execution_date.strftime("%Y_%m_%d_%H_%M_%S")}}/'
xls_to_xlsx_task = PythonOperator(
    task_id='xls_to_xlsx',
    python_callable=xls_to_xlsx,
    provide_context=True,
    op_kwargs={'input': xls_file_path,
               'output': xlsx_folder_path},
    dag=dag,
)

xlsx_file_path = './../data/processed/{{execution_date.strftime(' \
                 '"%Y_%m_%d_%H_%M_%S")}}/Vendas_de_Combustiveis_m3.xls '
sheets_folder_path = './../data/processed/{{execution_date.strftime(' \
                     '"%Y_%m_%d_%H_%M_%S")}}/Vendas_de_Combustiveis_m3_sheets/ '
xlsx_extract_sheets_to_csv_task = PythonOperator(
    task_id='xlsx_extract_sheets_to_csv',
    python_callable=xlsx_extract_sheets_to_csv,
    provide_context=True,
    op_kwargs={'input': xlsx_file_path,
               'output': sheets_folder_path},
    trigger_rule='all_success',
    dag=dag,
)

diesel_data_file_path = './../data/processed/{{execution_date.strftime(' \
                        '"%Y_%m_%d_%H_%M_%S")}}/Vendas_de_Combustiveis_m3_sheets/DPCache_m3.csv '
partitioned_data_folder_path = './../data/processed/{{execution_date.strftime(' \
                               '"%Y_%m_%d_%H_%M_%S")}}/Vendas_de_Combustiveis_m3_partitioned/ '
diesel_oil_table_transform_partition_task = PythonOperator(
    task_id='diesel_oil_table_transform_partition',
    python_callable=diesel_oil_table_transform_partition,
    provide_context=True,
    op_kwargs={'input': diesel_data_file_path,
               'output': partitioned_data_folder_path,
               'partitions': '',
               'capture_timestamp': ''},
    trigger_rule='all_success',
    dag=dag,
)
