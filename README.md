# PySpark Date Transformations

This library contains some common date transformations (YTD, MTD, PM, etc) for Spark dataframes. The transformations allows data comparison between dates periods


To use it:

Import the library: 


  `  from dateTransformation import applyDateTransformations`
  
From your dataframe identify the context columns (attributes), the columns with the numeric values that you want to transform (metrics) and the date column to use for the transformations.

Identify the transformations that you want to apply to the dataframe

Create a dataframe using the function applyDateTransformations with input parameters, your original dataframe, attributes, metrics, date column and required transformations.


```
    attributes = ['Operacion', 'Aplicacion', 'CPCD_Grupo', 'Grupo', 'Pai_Grupo', 'CPCD_Contrapartida', 'Contrapartida', 'Pais', 'Area', 'Sala', 'Area3', 'MesaFranquicia', 'GrupoProNuevo', 'Descripcion', 'frqBruta', 'Importe', 'GCC_Segmento', 'TIER', 'Nombre', 'Sala_MAN', 'Sala_Matriz', 'Sala_Filial'] 
    metrics = ['Importe','frqBruta']
    date_column = 'Fecha'        
    transformations = ['MTD','MTD_PY']

    transformed = applyDateTransformations (df_original, attributes, metrics, date_column, transformations)
```



The final dataframe will contain new columns with the transformations results.


List of supported transformations:
- YTD: Year to date.
- YTD_PY: Year to date compared to the previous year.
- MTD: Month to date.
- MTD_PY: Month to date compared to the previous year.
- PD: Previous day.
- PY: Previous day.
- LD: Last december
- PM: Same day previous month.
- PM_LD: Last day of previous month.



