from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from customFunctions import getTextColAnalysisRes, getContinuousColAnalysisRes, \
    getCategoricalOrDiscreteColAnalysisRes, getMissingCount, getFileColumnType, getFinalizesOutputRes

try:
    spark = SparkSession.builder.master("local[1]").appName("test_spark").config(
        "spark.sql.jsonGenerator.ignoreNullFields", "false").getOrCreate()
    # Reading csv files
    df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(
        "file:/home/shreyasm/Workspace/sparkTest/SampleTest.csv")
    print(df)
    file_columns_type = getFileColumnType(df)
    missing_col_values = getMissingCount(df)
    categorical_disccrete_val = getCategoricalOrDiscreteColAnalysisRes(df, file_columns_type)
    continuous_val = getContinuousColAnalysisRes(df, file_columns_type)
    text_val = getTextColAnalysisRes(df, file_columns_type)
    finalop = getFinalizesOutputRes(missing_col_values, file_columns_type, categorical_disccrete_val, continuous_val, text_val)

    # Generating output file
    spark.read.json(spark.sparkContext.parallelize([finalop])).coalesce(1).write.mode('overwrite').json(
        'file:/home/shreyasm/Workspace/sparkTest/spark_json_op')
except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    spark.stop()
