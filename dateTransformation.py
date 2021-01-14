from dateCalculation import ytd_table
from dateCalculation import mtd_table
from dateCalculation import pd_table
from dateCalculation import py_table
from dateCalculation import pm_table
from dateCalculation import previous_month_LD_table
from dateCalculation import last_december_table
from pyspark.sql import SparkSession

import pandas as pd
from datetime import datetime

from pyspark.sql.types import StructType, StructField, DateType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def applyDateTransformations (df_original,attributes, metrics, date_column, transformations):
    #df_original = Dataframe with the data you want to transform
    #attributes and metrics, list of columns
    #date_column = String with the name of the column that will be used as reference to all the transformations, ONLY ONE COLUMN and must be Date format.
    #transformations = List of transformations to apply, MTD, MTD_PY, YTD, YTD_PY
    
    spark = SparkSession.builder.appName("dateTransformation").getOrCreate()

    #Creating date Dataframes:
    max_date_row = df_original.select(date_column).groupBy().agg({date_column:'max'}).collect()
    min_date_row = df_original.select(date_column).groupBy().agg({date_column:'min'}).collect()
    max_date = max_date_row [0]['max(' + date_column + ')']
    min_date = min_date_row [0]['min(' + date_column + ')']
    datelist = pd.date_range(end = max_date, start = min_date)
    #datelist = pd.date_range(end = pd.datetime.date(datetime(2019,12,31)), periods=730)
    

    date_transformation_tables = 'Date_aux'

    if "MTD" in transformations or "MTD_PY" in transformations:
        df_mtd = mtd_table (datelist)
        
        for col in df_mtd.columns:
            df_mtd[col] = pd.Series(df_mtd[col].dt.strftime('%Y-%m-%d'))
        
        df_mtd = spark.createDataFrame(df_mtd)
        
        for col in df_mtd.columns:
            df_mtd = df_mtd.withColumn(col+'_aux',F.to_date(col))
            df_mtd = df_mtd.drop(col)    


    if "YTD" in transformations or "YTD_PY" in transformations:
        df_ytd = ytd_table (datelist)
        
        for col in df_ytd.columns:
            df_ytd[col] = pd.Series(df_ytd[col].dt.strftime('%Y-%m-%d'))   
        
        df_ytd = spark.createDataFrame(df_ytd)  
        
        for col in df_ytd.columns:
            df_ytd = df_ytd.withColumn(col+'_aux',F.to_date(col))
            df_ytd = df_ytd.drop(col)

            
    if "PY" in transformations:
        df_py = py_table (datelist)
        
        for col in df_py.columns:
            df_py[col] = pd.Series(df_py[col].dt.strftime('%Y-%m-%d'))   
        
        df_py = spark.createDataFrame(df_py)  
        
        for col in df_py.columns:
            df_py = df_py.withColumn(col+'_aux',F.to_date(col))
            df_py = df_py.drop(col)

    if "PD" in transformations:
        df_pd = pd_table (datelist)
        
        for col in df_pd.columns:
            df_pd[col] = pd.Series(df_pd[col].dt.strftime('%Y-%m-%d'))   
        
        df_pd = spark.createDataFrame(df_pd)  
        
        for col in df_pd.columns:
            df_pd = df_pd.withColumn(col+'_aux',F.to_date(col))
            df_pd = df_pd.drop(col)


    if "PM" in transformations:
        df_pm = pm_table (datelist)
        
        for col in df_pm.columns:
            df_pm[col] = pd.Series(df_pm[col].dt.strftime('%Y-%m-%d'))   
        
        df_pm = spark.createDataFrame(df_pm)  
        
        for col in df_pm.columns:
            df_pm = df_pm.withColumn(col+'_aux',F.to_date(col))
            df_pm = df_pm.drop(col)

    if "LD" in transformations:
        df_ld = last_december_table (datelist)
        
        for col in df_ld.columns:
            df_ld[col] = pd.Series(df_ld[col].dt.strftime('%Y-%m-%d'))   
        
        df_ld = spark.createDataFrame(df_ld)  
        
        for col in df_ld.columns:
            df_ld = df_ld.withColumn(col+'_aux',F.to_date(col))
            df_ld = df_ld.drop(col)
            
    if "PM_LD" in transformations:
        df_pm_ld = previous_month_LD_table (datelist)
        
        for col in df_pm_ld.columns:
            df_pm_ld[col] = pd.Series(df_pm_ld[col].dt.strftime('%Y-%m-%d'))   
        
        df_pm_ld = spark.createDataFrame(df_pm_ld)  
        
        for col in df_pm_ld.columns:
            df_pm_ld = df_pm_ld.withColumn(col+'_aux',F.to_date(col))
            df_pm_ld = df_pm_ld.drop(col)
            


    metrics_aux = metrics

    agg_dict = {}
    i = 0
    for metric in metrics:
        agg_dict[metrics[i]] = 'sum'
        i = i + 1 

    selects = []
    selects.append(date_transformation_tables)
    selects = selects + attributes
    grouping = selects
    selects = selects + metrics
    
    schema = df_original.schema
    df_original_mtd = spark.createDataFrame([],schema)
    df_original_mtd_aa = spark.createDataFrame([],schema)
    df_original_ytd = spark.createDataFrame([],schema)
    df_original_ytd_aa = spark.createDataFrame([],schema)
    df_original_py = spark.createDataFrame([],schema)
    df_original_pd = spark.createDataFrame([],schema)
    df_original_pm = spark.createDataFrame([],schema)
    df_original_ld = spark.createDataFrame([],schema)
    df_original_pm_ld = spark.createDataFrame([],schema)

    if "MTD" in transformations:
        suffix = '_mtd'
        df_original_mtd = df_original.join(df_mtd, df_original[date_column] == df_mtd.MTD_aux, 'left')
        for metric in metrics:
            df_original_mtd = df_original_mtd.withColumnRenamed(metric,metric+suffix)
            metrics_aux = metrics_aux + [metric+suffix]
        df_original_mtd = df_original_mtd.drop(date_column)
        df_original_mtd = df_original_mtd.withColumnRenamed (date_transformation_tables,date_column)

    if "MTD_PY" in transformations:
        suffix = '_mtd_py'
        df_original_mtd_aa = df_original.join(df_mtd, df_original[date_column] == df_mtd.MTD_PY_aux, 'left')
        for metric in metrics:
            df_original_mtd_aa = df_original_mtd_aa.withColumnRenamed(metric,metric+suffix)
            metrics_aux = metrics_aux + [metric+suffix]
        df_original_mtd_aa = df_original_mtd_aa.drop(date_column)
        df_original_mtd_aa = df_original_mtd_aa.withColumnRenamed (date_transformation_tables,date_column)            

    if "YTD" in transformations:
        suffix = '_ytd'
        df_original_ytd = df_original.join(df_ytd, df_original[date_column] == df_ytd.YTD_aux, 'left')
        for metric in metrics:
            df_original_ytd = df_original_ytd.withColumnRenamed(metric,metric+suffix)
            metrics_aux = metrics_aux + [metric+suffix]
        df_original_ytd = df_original_ytd.drop(date_column)
        df_original_ytd = df_original_ytd.withColumnRenamed (date_transformation_tables,date_column)

    if "YTD_PY" in transformations:
        suffix = '_ytd_py'
        df_original_ytd_aa = df_original.join(df_ytd, df_original[date_column] == df_ytd.YTD_PY_aux, 'left')
        for metric in metrics:
            df_original_ytd_aa = df_original_ytd_aa.withColumnRenamed(metric,metric+suffix)
            metrics_aux = metrics_aux + [metric+suffix]
        df_original_ytd_aa = df_original_ytd_aa.drop(date_column)
        df_original_ytd_aa = df_original_ytd_aa.withColumnRenamed (date_transformation_tables,date_column)

    if "PY" in transformations:
        suffix = '_py'
        df_original_py = df_original.join(df_py, df_original[date_column] == df_py.PY_aux, 'left')
        for metric in metrics:
            df_original_py = df_original_py.withColumnRenamed(metric,metric+suffix)
            metrics_aux = metrics_aux + [metric+suffix]
        df_original_py = df_original_py.drop(date_column)
        df_original_py = df_original_py.withColumnRenamed (date_transformation_tables,date_column)

    if "PD" in transformations:
        suffix = '_pd'
        df_original_pd = df_original.join(df_pd, df_original[date_column] == df_pd.PD_aux, 'left')
        for metric in metrics:
            df_original_pd = df_original_pd.withColumnRenamed(metric,metric+suffix)
            metrics_aux = metrics_aux + [metric+suffix]
        df_original_pd = df_original_pd.drop(date_column)
        df_original_pd = df_original_pd.withColumnRenamed (date_transformation_tables,date_column)

    if "PM" in transformations:
        suffix = '_pm'
        df_original_pm = df_original.join(df_pm, df_original[date_column] == df_pm.PM_aux, 'left')
        for metric in metrics:
            df_original_pm = df_original_pm.withColumnRenamed(metric,metric+suffix)
            metrics_aux = metrics_aux + [metric+suffix]
        df_original_pm = df_original_pm.drop(date_column)
        df_original_pm = df_original_pm.withColumnRenamed (date_transformation_tables,date_column)        

    if "LD" in transformations:
        suffix = '_ld'
        df_original_ld = df_original.join(df_ld, df_original[date_column] == df_ld.LD_aux, 'left')
        for metric in metrics:
            df_original_ld = df_original_ld.withColumnRenamed(metric,metric+suffix)
            metrics_aux = metrics_aux + [metric+suffix]
        df_original_ld = df_original_ld.drop(date_column)
        df_original_ld = df_original_ld.withColumnRenamed (date_transformation_tables,date_column)

    if "PM_LD" in transformations:
        suffix = '_pm_ld'
        df_original_pm_ld = df_original.join(df_pm_ld, df_original[date_column] == df_pm_ld.PM_LD_aux, 'left')
        for metric in metrics:
            df_original_pm_ld = df_original_pm_ld.withColumnRenamed(metric,metric+suffix)
            metrics_aux = metrics_aux + [metric+suffix]
        df_original_pm_ld = df_original_pm_ld.drop(date_column)
        df_original_pm_ld = df_original_pm_ld.withColumnRenamed (date_transformation_tables,date_column)


    for metric in metrics_aux:
        if metric not in df_original.columns:
            df_original = df_original.withColumn(metric,F.lit(0))

        if metric not in df_original_mtd.columns:
            df_original_mtd = df_original_mtd.withColumn(metric,F.lit(0))

        if metric not in df_original_mtd_aa.columns:
            df_original_mtd_aa = df_original_mtd_aa.withColumn(metric,F.lit(0)) 

        if metric not in df_original_ytd.columns:
            df_original_ytd = df_original_ytd.withColumn(metric,F.lit(0)) 

        if metric not in df_original_ytd_aa.columns:
            df_original_ytd_aa = df_original_ytd_aa.withColumn(metric,F.lit(0)) 

        if metric not in df_original_py.columns:
            df_original_py = df_original_py.withColumn(metric,F.lit(0))

        if metric not in df_original_pd.columns:
            df_original_pd = df_original_pd.withColumn(metric,F.lit(0)) 

        if metric not in df_original_pm.columns:
            df_original_pm = df_original_pm.withColumn(metric,F.lit(0))

        if metric not in df_original_ld.columns:
            df_original_ld = df_original_ld.withColumn(metric,F.lit(0)) 

        if metric not in df_original_pm_ld.columns:
            df_original_pm_ld = df_original_pm_ld.withColumn(metric,F.lit(0))        


    agg_dict = {}
    i = 0
    for metric in metrics_aux:
        agg_dict[metrics_aux[i]] = 'sum'
        i = i + 1 

    selects = []
    selects.append(date_column)
    selects = selects + attributes
    grouping = selects
    selects = selects + metrics_aux
    df_original_final = df_original.select(selects).groupBy(grouping).agg(agg_dict)


    df_original_mtd_final = df_original_mtd.select(selects).groupBy(grouping).agg(agg_dict)
    df_original_mtd_aa_final = df_original_mtd_aa.select(selects).groupBy(grouping).agg(agg_dict)
    df_original_ytd_final = df_original_ytd.select(selects).groupBy(grouping).agg(agg_dict)
    df_original_ytd_aa_final = df_original_ytd_aa.select(selects).groupBy(grouping).agg(agg_dict)
    df_original_py_final = df_original_py.select(selects).groupBy(grouping).agg(agg_dict)
    df_original_pd_final = df_original_pd.select(selects).groupBy(grouping).agg(agg_dict)
    df_original_pm_final = df_original_pm.select(selects).groupBy(grouping).agg(agg_dict)
    df_original_ld_final = df_original_ld.select(selects).groupBy(grouping).agg(agg_dict)
    df_original_pm_ld_final = df_original_pm_ld.select(selects).groupBy(grouping).agg(agg_dict)



    df_total = df_original_final

    df_total = df_total.union (df_original_mtd_final)
    df_total = df_total.union (df_original_mtd_aa_final)
    df_total = df_total.union (df_original_ytd_final)
    df_total = df_total.union (df_original_ytd_aa_final)
    df_total = df_total.union (df_original_py_final)
    df_total = df_total.union (df_original_pd_final)
    df_total = df_total.union (df_original_pm_final)
    df_total = df_total.union (df_original_ld_final)
    df_total = df_total.union (df_original_pm_ld_final)

    df_total = rename_columns(df_total)
    df_total = df_total.select(selects).groupBy(grouping).agg(agg_dict) #added to group the union of all previous dataframes.
    df_total = rename_columns(df_total)
   
    df_total = df_total.where(df_total[date_column].isNotNull()) #remove null date columns created when there is no previous data?¿

    return df_total
    
#####################################################################
def rename_columns (df_total):
# Renaming columns to remove the aggregation formula
    columns = df_total.columns
    
    for column in columns:
        if column.find('sum(') != -1:
            column_aux = column.replace ('sum(','')
            column_aux = column_aux.replace (')','')
            df_total = df_total.withColumnRenamed (column, column_aux)
    return df_total
####################################################################


if __name__=='__main__':
    attributes = ['Operacion', 'Aplicacion', 'CPCD_Grupo', 'Grupo', 'Pai_Grupo', 'CPCD_Contrapartida', 'Contrapartida', 'Pais', 'Area', 'Sala', 'Area3', 'MesaFranquicia', 'GrupoProNuevo', 'Descripcion', 'frqBruta', 'Importe', 'GCC_Segmento', 'TIER', 'Nombre', 'Sala_MAN', 'Sala_Matriz', 'Sala_Filial'] 
    metrics = ['Importe','frqBruta']
    date_column = 'Fecha'        
    transformations = ['MTD','MTD_PY']

    transformed = applyDateTransformations (df_original, attributes, metrics, date_column, transformations)