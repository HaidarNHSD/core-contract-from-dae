# Databricks notebook source
#%run ../main

# COMMAND ----------

#### Check final CSV has right columns

# COMMAND ----------

check_name = 'Intital check - all correct columns in the final csv'
right_columns = ["QUALITY_SERVICE","PRACTICE_CODE", "PRACTICE_NAME","ACH_DATE","IND_CODE","MEASURE","MEASURE_TYPE","VALUE"]

try:
    assert main_supp_out.columns == right_columns
    log_test("QA CHECK PASSED: " + check_name)
    
except AssertionError:
    log_test('QA CHECK FAILURE: '+check_name)    

# COMMAND ----------

#### Check final CSV has suppression


# COMMAND ----------

check_name = 'Intital check - Check if suppressed'

suppression_count = main_supp_out.filter(main_supp_out["VALUE"] == "*").count()

try:
    assert suppression_count > 0
    log_test("QA CHECK PASSED: " + check_name)
    
except AssertionError:
    log_test('QA CHECK FAILURE: '+check_name)   

# COMMAND ----------

#### Check final CSV has correct amount of rows

# COMMAND ----------

check_name = 'Intital check - Check number of rows is > 6000'

row_count = main_supp_out.count()

try:
    assert row_count >= 6000
    log_test("QA CHECK PASSED: " + check_name)
    
except AssertionError:
    log_test('QA CHECK FAILURE: '+check_name)   

# COMMAND ----------

#### Check final CSV has valid data

# COMMAND ----------

##fix this
check_name = 'Intital check - Check valid data'

row_count = main_supp_out.count()

try:
    assert row_count >= 6000
    log_test("QA CHECK PASSED: " + check_name)
    
except AssertionError:
    log_test('QA CHECK FAILURE: '+check_name)  