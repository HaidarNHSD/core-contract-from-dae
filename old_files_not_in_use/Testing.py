# Databricks notebook source
# MAGIC %md
# MAGIC ### Deprecated File, please use testing_data in tests file

# COMMAND ----------

def gmspmstest1(final_df: DataFrame) -> int:

    """
    This function conducts the first test if checking the number of unique practices in the source data
    
    Parameters:
        final_df: Processed Spark DataFrame.
    
    Returns:
        sourceNoPractices : Number of practices in source
        outputNoPractices : Number of practices in output
    """
  
    sourceNopractices = spark.sql(f"""
        SELECT COUNT(DISTINCT ORG_CODE) FROM  prim_core_contract.core_contract_cc_store
        WHERE  ACH_DATE >= '{report_period_start}'
        AND ACH_DATE <= '{report_period_end}'
    """).first()['count(DISTINCT ORG_CODE)']


    outputNoPractices = final_df.select('PRACTICE_CODE').distinct().count()
    print('sourceNopractices : ', sourceNopractices)
    print('outputNoPractices : ', outputNoPractices)
    
    return sourceNopractices, outputNoPractices

# COMMAND ----------

def gmspmstest2(sourceNopractices : DataFrame, outputNoPractices: DataFrame) -> str:
  
    """
    This function conducts the second test of checking if the number of output practices matches the number
    of source practices
    
    Parameters:
        sourceNoPractices : Number of practices in source
        outputNoPractices : Number of practices in output
    
    Returns:
        check2
    """
    check2 = sourceNopractices == outputNoPractices  
    
    
    if check2:
      print(f'Matched with practice count in database = {sourceNopractices} practices') 
    else:
      print(f'Did not Match with practice count in database')   
      
    return check2

# COMMAND ----------

def gmspmstest3(final_df: DataFrame) -> str:
  
    """
    This function conducts the third test of checking the same number of indicators per practice
    
    Parameters:
        final_df: Processed Spark DataFrame.
    
    Returns:
        check3
    """
    
    
    # Get number of unique indicators in database
    noOfIndicators = spark.sql(f"""
        SELECT COUNT(DISTINCT IND_CODE) as count FROM  prim_core_contract.core_contract_cc_store
        WHERE ACH_DATE >= '{report_period_start}' 
        AND ACH_DATE <= '{report_period_end}'
    """).first()['count']

    print("Number of Indicators in Database:", noOfIndicators)

    # Get number of indicators for each practice
    indPerPractice= final_df.groupBy('PRACTICE_CODE').agg(countDistinct('IND_CODE'))

    # Checking whether it is this same number of indicator codes in each practice
    check3 = indPerPractice.filter(col('count(IND_CODE)')!= noOfIndicators).count() == 0
    if check3:
      print("Each practice has the same number of indicator codes")
    else:  
      print("Not all practices have the same number of indicator codes")    
      
    return check3

# COMMAND ----------

def tests(final_df: DataFrame) -> None:
  
    """
    This combines all the three test outputs into a single dataframe for exporting
    
    Parameters:
        final_df: Processed Spark DataFrame.
        
    Returns:
        test_df : Spark DataFrame which has all three test ouputs inside
    """
    
    #Obtaining required outputs from all three test functions
    sourceNoPractices, outputNoPractices = gmspmstest1(final_df)
    check2 = gmspmstest2(sourceNoPractices, outputNoPractices)
    check3 = gmspmstest3(final_df)
    
    #Form columns and values
    columns = ['TESTS', 'PASS/FAIL']
    values  =[
      ('Number of unique practices  ' ,str(sourceNoPractices)),
      ('Number of output practices matches the number of source practices', str(check2)),
      ('Same number of indicator codes per practice', str(check3)),
    ]
    
    print('Number of unique practices' ,outputNoPractices)
    test_df = spark.createDataFrame(values, columns)
    display(test_df)    
    return test_df
    