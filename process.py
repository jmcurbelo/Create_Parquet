import paths
import pandas as pd
from pyspark.sql.types import *
import numpy as np
import ipywidgets as widgets
from ipywidgets import interact, interact_manual
from IPython.display import display
import findspark
import pyspark
from pyspark.sql import SQLContext
findspark.init()
sc = pyspark.SparkContext()
spark = SQLContext(sc)

pd.options.mode.chained_assignment = None


def CreateParquet(path, schema_path, partition, output_path):
    
    df = pd.read_csv(path, sep = '|', index_col=False, skipinitialspace=False, dtype=str)
    
    columnas = df.columns.tolist()
    columnasA = columnas[1:(len(columnas) -1)]
    new_df = df.iloc[:, 1:(len(columnas) -1)]
    columnasD = [x.strip(' ') for x in columnasA]
    new_df.columns = columnasD
    
    schema = pd.read_csv(schema_path, sep = '\n', index_col=False, header=None)
    schema.columns = ['schema']
    schema[['nombre', 'type']] = schema['schema'].str.split(': ', 1, expand=True)
    
    types = schema['type'].tolist()

    for i in zip(columnasD,types):

        to_clear = new_df[i[0]].tolist()
        clear = ['' if x.strip(' ') == 'null' else x.strip() for x in to_clear]
        new_df[i[0]] = clear
        del to_clear[:]
        del clear[:]

        if (i[1] == 'string'):
            new_df[i[0]] = new_df[i[0]].astype(str)
            new_df[i[0]] = new_df[i[0]].str.strip()
        if (i[1][0:7] == 'decimal'):
            new_df[i[0]] =  new_df[i[0]].astype(np.float64)
        if (i[1] == 'date'):
            new_df[i[0]] = new_df[i[0]].astype(str)
            new_df[i[0]] = new_df[i[0]].str.strip()
            new_df[i[0]] =  pd.to_datetime(new_df[i[0]], format='%Y-%m-%d')
        if (i[1] == 'timestamp'):
            new_df[i[0]] =  pd.to_datetime(new_df[i[0]], format='%Y-%m-%d %H:%M:%S')
        if (i[1] == 'integer'):
            new_df[i[0]] =  new_df[i[0]].astype(np.int32)
        if (i[1] == 'long'):
            new_df[i[0]] =  new_df[i[0]].astype(np.int64)

            
    field = []

    for i in zip(columnasD,types):
        if (i[1] == 'string'):
            field.append(StructField(i[0], StringType(), True))
        if (i[1][0:7] == 'decimal'):
            field.append(StructField(i[0], DoubleType(), True))
        if (i[1] == 'date'):
            field.append(StructField(i[0], DateType(), True))
        if (i[1] == 'timestamp'):
            field.append(StructField(i[0], TimestampType(), True))
        if (i[1] == 'integer'):
            field.append(StructField(i[0], IntegerType(), True))
        if (i[1] == 'long'):
            field.append(StructField(i[0], LongType(), True))
            
    schema_spk = StructType(field)
    
    df_spark = spark.createDataFrame(data=new_df,schema=schema_spk)
    
    df_spark.coalesce(1).write.mode('overwrite').partitionBy(partition).parquet(output_path)


CreateParquet(paths.sample, paths.schema, paths.partition, paths.output)