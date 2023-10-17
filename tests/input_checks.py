# Databricks notebook source
# More input checks can be added but for now, only 2 are coded in.

# COMMAND ----------

# MAGIC %run ../functions/pre_processing

# COMMAND ----------

from pyspark.sql.functions import isnull, sum
#Run checks on the DAE databases to see if they work

# COMMAND ----------

def test_connection(which_conn : int) -> DataFrame:
    """ Test the connection to the database.
    Parameters:
    which_conn : 0 for gmspmstable, 1 for prac_df
    Returns:
    df - The DataFrame obtained from connection """
    
    if which_conn == 0:
        df = get_gmspmstable()
        df_name = "gmspms_table"
    elif which_conn == 1:
        df = get_pracdf()
        df_name = "pracdf"
        
    #gms_pms_table = spark.table('prim_core_contract.core_contract_cc_store')
    check_name = 'Test connection - '
    try:
        assert df.count() > 0 and len(df.columns) > 0
        log_test("INPUT CHECK PASSED: " + check_name + df_name)
    except AssertionError:
        log_test("INPUT CHECK FAILED: " + check_name + df_name)
        
    return df
    

# COMMAND ----------

def check_columns(df : DataFrame, correct_columns : list, df_name : str) -> None:
    """ Test the connection to the database.
    Parameters:
    df - Input DataFrame to check
    correct_columns : A list of the correct columns
    df_name - Input DataFrame name
    Returns:
    None """
    
    check_name = 'Input Check - check columns - '

    try:
        assert df.columns == correct_columns
        log_test("INPUT CHECK PASSED: " + check_name + df_name) 
    except AssertionError:
        log_test("INPUT CHECK FAILURE: " + check_name + df_name)

# COMMAND ----------

def check_nulls(df : DataFrame, df_name : str) -> None:
    """ Test the connection to the database.
    Parameters:
    df - Input DataFrame to check
    df_name - Input DataFrame name
    Returns:
    None """
    
    check_name = 'Input Check - check for N/A values - '
    null_counts = df.select([(sum(isnull(c).cast("int")).alias(c)) for c in df.columns]).collect()
    null_counts_dict = {c: null_counts[0][c] for c in df.columns}
    print(null_counts_dict)
    total = 0
    for nulls in null_counts_dict.values():
        total += nulls

    try:
        assert total == 0
        log_test("INPUT CHECK PASSED: " + check_name + df_name) 

    except AssertionError:
        log_test("INPUT CHECK FAILURE: " + check_name + df_name)       


# COMMAND ----------

gms_pms_table = test_connection(0)
check_columns(gms_pms_table,['QUALITY_SERVICE', 'ORG_CODE','ACH_DATE','AID','IND_CODE','MEASURE','VALUE','DATESTAMP'], 'gms_pms_table')
check_nulls(gms_pms_table, 'gms_pms_table')

# COMMAND ----------

prac_df = test_connection(1)
check_columns(prac_df , ['CODE', 'NAME'], 'prac_df')
check_nulls(prac_df, 'prac_df')

# COMMAND ----------

#I have to delete this module as it breaks the suppression testing code
del sum