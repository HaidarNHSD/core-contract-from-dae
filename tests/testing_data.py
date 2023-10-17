# Databricks notebook source
#%run ./function_test_suite

# COMMAND ----------

#%run ../functions/run_functions

# COMMAND ----------

def gmspmstest1(final_df: DataFrame) -> int:

    """
    This function conducts the first test if checking the number of unique practices in the source data
    
    Parameters:
        final_df: Processed Spark DataFrame.
    
    Returns:
        sourceNoPractices : Number of practices in source
    """
  
    sourceNopractices = spark.sql(f"""
        SELECT COUNT(DISTINCT ORG_CODE) FROM  prim_core_contract.core_contract_cc_store
        WHERE  ACH_DATE >= '{report_period_start}'
        AND ACH_DATE <= '{report_period_end}'
    """).first()['count(DISTINCT ORG_CODE)']

    outputNoPractices = final_df.select('PRACTICE_CODE').distinct().count()
    log_info('sourceNopractices : ' + str(sourceNopractices))
    log_info('outputNoPractices : ' + str(outputNoPractices))
    
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
        log_info(f'Matched with practice count in database = {sourceNopractices} practices') 
    else:
        log_info(f'Did not Match with practice count in database')   
      
    #assert check2 == True
    
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

    log_info("Number of Indicators in Database:" + str(noOfIndicators))

    # Get number of indicators for each practice
    indPerPractice= final_df.groupBy('PRACTICE_CODE').agg(countDistinct('IND_CODE'))

    # Checking whether it is this same number of indicator codes in each practice
    check3 = indPerPractice.filter(col('count(IND_CODE)')!= noOfIndicators).count() == 0
    if check3:
        log_test("Each practice has the same number of indicator codes")
    else: 
        log_test("Not all practices have the same number of indicator codes")   
    
    return check3

# COMMAND ----------

test_suite_instance = FunctionTestSuite()
@FunctionTestSuite.add_test
def asserttests() -> None:
  
  
    final_df = data_ingestion(print_tables=False)
        
    #Obtaining required outputs from all three test functions
    sourceNoPractices, outputNoPractices = gmspmstest1(final_df)
    try: 
        assert sourceNoPractices > 6000
        log_test("Test 1 passed: Number of Practices in source is over 6000")
    except AssertionError:
        log_test("Number of practices expected > 6000, please check if this is correct.")
    
    
    check2 = gmspmstest2(sourceNoPractices,outputNoPractices)
    try: 
        assert check2 == True
        log_test("Test 2 passed: check 2 sucessful")
    except AssertionError:
        log_test("Test 2 failed")
        
    check3 = gmspmstest3(final_df)
    try: 
        assert check3 == True
        log_test("Test 3 passed: check 3 successful")
    except AssertionError:
         log_test("Test 3 failed")
        
    #Form columns and values
    columns = ['TESTS', 'PASS/FAIL']
    values  =[
      ('Number of unique practices  ' ,str(sourceNoPractices)),
      ('Number of output practices matches the number of source practices', str(check2)),
      ('Same number of indicator codes per practice', str(check3)),
    ]
    
    test_df = spark.createDataFrame(values, columns)
    display(test_df)    
    return test_df
    

# COMMAND ----------

if __name__ == '__main__':
    test_suite_instance.run()
    #unittest.main(argv=['first-arg-is-ignored'], exit =False)     