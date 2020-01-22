# coding=utf-8

import subprocess
import pandas as pd
import os
import pyarrow.parquet as pq
import pyarrow as pa
import datetime as dt


def xls_to_xlsx(input, output, **context):
    """
    Converts a xls file to xlsx. This process extracts caches from pivots and saves them as sheets
    :param input: xls file path.
    :type input: str
    :param output: xlsx output file path.
    :type output: str
    """

    bashCommand = "libreoffice --headless --convert-to xlsx --outdir {OUTPUT} {INPUT}".format(OUTPUT=output,
                                                                                              INPUT=input)
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()


def xlsx_extract_sheets_to_csv(input, output, **context):
    """
    Converts each sheet of a xlsx file to csv.
    :param input: xlsx file path.
    :type input: str
    :param output: folder path to output generated csvs.
    :type output: str
    """

    xl = pd.ExcelFile(input)
    os.mkdir(output)
    for sheet in xl.sheet_names:
        df = pd.read_excel(xl, sheet, decimal=',')
        df.to_csv('{OUTPUT}{SHEET}.csv'.format(OUTPUT=output, SHEET=sheet), sep='|', decimal='.')


def diesel_oil_table_transform_partition(input, output, partitions, capture_timestamp, **context):
    """
    Receives a csv file as input, do some transformation in data, adds timestamp_captura column and saves the result
    and saves the result dataframe partitioned by some columns in parquet format.
    :param input: csv file.
    :type input: str
    :param output: folder path to output partitioned table in parquet.
    :type output: str
    :param partitions: columns to partition data by.
    :type partitions: list
    :param capture_timestamp: capture timestamp.
    :type capture_timestamp: datetime
    """
    dtypes = {'COMBUSTÍVEL': 'str', 'ANO': 'int', 'REGIÃO': 'str', 'ESTADO': 'str', 'Jan': 'float', 'Fev': 'float',
              'Mar': 'float', 'Abr': 'float', 'Mai': 'float', 'Jun': 'float', 'Jul': 'float', 'Ago': 'float',
              'Set': 'float', 'Out': 'float',
              'Nov': 'float', 'Dez': 'float', }
    columns = {'COMBUSTÍVEL': 'produto', 'ANO': 'ano', 'REGIÃO': 'unidade', 'ESTADO': 'estado', 'Jan': '1', 'Fev': '2',
               'Mar': '3', 'Abr': '4', 'Mai': '5', 'Jun': '6', 'Jul': '7', 'Ago': '8', 'Set': '9', 'Out': '10',
               'Nov': '11', 'Dez': '12', }
    df = pd.read_csv(input, index_col=0, dtype=dtypes, sep='|', decimal='.')
    df = df.drop(['TOTAL'], axis=1)
    df = df.rename(columns=columns)
    df = df.melt(id_vars=['produto', 'ano', 'unidade', 'estado'], var_name='mes', value_name='vol_demanda_m3')
    df['timestamp_captura'] = capture_timestamp
    df.head(10)
    table = pa.Table.from_pandas(df)
    print(table.schema)
    pq.write_to_dataset(table, root_path=output,
                        partition_cols=partitions)


if __name__ == "__main__":
    execution_time = dt.datetime.now()
    execution_time_path = execution_time.strftime('%Y_%m_%d_%H_%M_%S')
    xls_to_xlsx('./../../data/Vendas_de_Combustiveis_m3.xls',
                './../../data/processed/{EXECUTION_TIME}/'.format(EXECUTION_TIME=execution_time_path))

    xlsx_extract_sheets_to_csv('./../../data/processed/{EXECUTION_TIME}/Vendas_de_Combustiveis_m3.xlsx'.format(
        EXECUTION_TIME=execution_time_path),
        './../../data/processed/{EXECUTION_TIME}/Vendas_de_Combustiveis_m3_sheets/'.format(
            EXECUTION_TIME=execution_time_path))

    diesel_oil_table_transform_partition(
        './../../data/processed/{EXECUTION_TIME}/Vendas_de_Combustiveis_m3_sheets/DPCache_m3.csv'.format(
            EXECUTION_TIME=execution_time_path),
        './../../data/processed/{EXECUTION_TIME}/Vendas_de_Combustiveis_m3_partitioned/'.format(
            EXECUTION_TIME=execution_time_path),
        ['ano', 'mes'], execution_time)
