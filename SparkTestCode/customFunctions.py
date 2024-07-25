from pyspark.sql.functions import *


def getFileColumnType(df):
    df_col = df.columns
    col_types = dict()
    col_types['discrete'] = []
    col_types['categorical'] = []
    col_types['continuous'] = []
    col_types['text'] = []
    # Decide category of column based on below logic-
    # Categorical fields decided based on threshold value which can be adjusted
    # Field types are decided based on datatypes
    for column in df_col:
        dtype = df.schema[column].dataType.typeName()
        threshold = df.count() * 0.05  # 0.05 can be adjusted
        if dtype == "integer":
            if df.select(column).distinct().count() < threshold:
                col_types['categorical'].append(column)
            else:
                col_types['discrete'].append(column)
        elif dtype == "double" or dtype == "float":
            col_types['continuous'].append(column)
        elif dtype == "string":
            if df.select(column).distinct().count() < threshold:
                col_types['categorical'].append(column)
            else:
                col_types['text'].append(column)

    return col_types


def getMissingCount(df):
    # Get count of missing values for each column in dataframe
    dict_null = {col: df.filter(df[col].isNull()).count() for col in df.columns}
    return dict_null


def getCategoricalOrDiscreteColAnalysisRes(df, val1):
    # Get distinct values of columns
    # (Categorical and discrete type of columns) and its frequency in dataframe
    discrete_col = val1['discrete']
    categorical_col = val1['categorical']
    final_list_col = discrete_col + categorical_col
    dict_categorical_or_discrete_op_res = dict()
    for col in final_list_col:
        dict_categorical_or_discrete_op_res[col] = df.na.fill("null").groupby(col).count().toJSON().collect()
    return dict_categorical_or_discrete_op_res


def getContinuousColAnalysisRes(df, val1):
    # Get Min and Max values for Continuous columns in dataframe
    # Get histogram details for Continuous columns in dataframe
    continuous_col = val1['continuous']
    continuous_analysis_details = dict()
    min_max_list = []
    hist_list = []
    for col in continuous_col:
        result = df.select([min(col), max(col)])
        min_max_list.append(result.toJSON().collect())

        hist = df.select(histogram_numeric(col, lit(5))).toJSON().collect()
        hist_list.append(hist)

    continuous_analysis_details['continuous_col_min_max_details'] = min_max_list
    continuous_analysis_details['continuous_col_histogram_details'] = hist_list
    return continuous_analysis_details


def getTextColAnalysisRes(df, val1):
    # Get distinct count of words present in Text type columns in dataframe
    textCol1 = val1['text']
    final_res = []
    for column1 in textCol1:
        cnt_var = column1 + '_distinct_word_cnt'
        df1 = df.withColumn(cnt_var, size(split(col(column1), ' '))).select(column1, cnt_var)
        df2 = df1.select(cnt_var).distinct().toJSON().collect()
        final_res.append(df2)
    return final_res


def getFinalizesOutputRes(MissingColValues, fileColumnsType, CategoricalDisccreteVal, ContinuousVal, TextVal):
    final_op = dict()
    final_op["MissingColValues"] = MissingColValues
    final_op["ColumnCategories"] = fileColumnsType
    final_op["CategoricalOrDiscreteColAnalysis"] = CategoricalDisccreteVal
    final_op["ContinuousColAnalysis"] = ContinuousVal
    final_op["TextColAnalysis"] = TextVal
    return final_op
