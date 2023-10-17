# Databricks notebook source
# MAGIC %md
# MAGIC ### Note: File deprecated, please use test_suppression and testing_data to run unitttests

# COMMAND ----------

# MAGIC %run ./function_test_suite

# COMMAND ----------

# MAGIC %run ../functions/run_functions

# COMMAND ----------

# MAGIC %run ./test_suppression

# COMMAND ----------

# MAGIC %run ./testing_data

# COMMAND ----------

if __name__ == '__main__':
    test_suite_instance.run()
    #unittest.main(argv=['first-arg-is-ignored'], exit =False)     