# Databricks notebook source
import unittest
from typing import *
from functools import wraps
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

def clear_cache():
  spark.sql('CLEAR CACHE')

# COMMAND ----------

class FunctionTestSuite(object):

  def __init__(self):
    self._suite = unittest.TestSuite()
    self._runner = unittest.TextTestRunner()
    
  @classmethod
  def add_test(self, test_func: Callable[[None], bool]) -> None:
    @wraps(test_func)
    def clean_up_func():
      result = test_func()
      clear_cache()
      return result
    
    test_suite_instance._suite.addTest(unittest.FunctionTestCase(clean_up_func))
   
  ##database connection code from JC, didnt use here in job
  def setUpClass(self):
    conn_string = ("Driver=SQL Server;"
                   "Server=DSSPROD;"
                   "Database=DSS_CORPORATE;"
                   "Trusted_Connection=yes;")
    self.conn = connect(conn_string)
    self.ach_date = "2021-07-01"
  def test_connect(self):
      conn_string = ("Driver=SQL Server;"
             "Server=DSSPROD;"
             "Database=DSS_CORPORATE;"
             "Trusted_Connection=yes;")
      conn = connect(conn_string)
      self.assertIsNotNone(conn)  
  ##
  
  def run(self):
    result = self._runner.run(self._suite)
    if not result.wasSuccessful():
      raise AssertionError("Tests failed!")

# COMMAND ----------

def flatten_struct_columns(nested_df):
  """
  check_content_match doesn't work on struct columns.
  So we need to split them up into columns before we can compare the dataframes.
  """
  stack = [((), nested_df)]
  columns = []
  while len(stack) > 0:
    parents, df = stack.pop()
    for column_name, column_type in df.dtypes:
      if column_type[:6] == "struct":
        projected_df = df.select(column_name + ".*")
        stack.append((parents + (column_name,), projected_df))
      else:
        columns.append(F.col(".".join(parents + (column_name,))).alias("_".join(parents + (column_name,))))
  return nested_df.select(columns)

# COMMAND ----------

def check_schemas_match(df1: DataFrame,
                        df2: DataFrame,
                        allow_nullable_schema_mismatch=True
                       ) -> bool:
  """
  Returns True if the dataframe schemas match, or False otherwise.
  
  If allow_nullable_schema_mismatch is False then the nullability of the columns must also match.
  If True, nullability isn't included in the check.
  """
  
  if df1.schema == df2.schema:
    return True
  elif not allow_nullable_schema_mismatch:
    print('allow_nullable_schema_mismatch')
    return False
  
  if len(df1.schema) != len(df2.schema):
    print('schema length mismatch')    
    return False
  
  for field_1, field_2 in zip(df1.schema, df2.schema):
    if field_1.name != field_2.name:
      print('name error', field_1, field_2)      
      return False
    if field_1.dataType != field_2.dataType:
      print('datatype error', field_1, field_2)
      
      return False
    
  return True 

# COMMAND ----------

def check_content_match(df1: DataFrame,
                         df2: DataFrame,
                         join_col: List[str]
                        ) -> bool:
  
  """
  Compares the values in the common columns only.
  An outer join on the given join_cols is used to decide which records to compare.
  """
  join_condition = [df1[c].eqNullSafe(df2[c]) for c in join_col]
  df3 = df1.alias("d1").join(df2.alias("d2"), join_condition, "outer")
  #df3.show()
  if df1.count() == df2.count():
    for name in set(df1.columns).intersection(set(df2.columns)):
      df3 = df3.withColumn(name + "_diff", F.when((F.col("d1." + name).isNull() & F.col("d2." + name).isNotNull()) |
                                                  (F.col("d1." + name).isNotNull() & F.col("d2." + name).isNull()), 1) \
                                            .when(F.col("d1." + name) != F.col("d2." + name), 1) \
                                            .otherwise(0))
    col_diff = [_col for _col in df3.columns if '_diff' in _col]
    diff_sum = df3.select(col_diff).groupBy().sum().first()
    
    df4 = df3.select(col_diff).groupBy().sum()
    mismatches_by_col_dict=df4.collect()[0].asDict()
    #print(mismatches_by_col_dict)
    for key, value in mismatches_by_col_dict.items():
      key_formatted = key.replace('sum(', '').replace(")", "")
      if value!=0:
        print("Content does not match in column", key_formatted)
    #print(diff_sum.asDict().values())
    if sum(diff_sum.asDict().values()) == 0:
      res = True
    else:
      res = False
      print('Content not match.', diff_sum)
  else:
    res = False
    print('Content not match.')
  return res

# COMMAND ----------

def compare_results(df1: DataFrame,
                    df2: DataFrame,
                    join_columns: List,
                    allow_nullable_schema_mismatch=True
                   ) -> bool:
  """
  Compare two dataframes. Used in testing to check outputs match expected outputs.
  """
  df1 = flatten_struct_columns(df1)
  df2 = flatten_struct_columns(df2)
  
  if check_schemas_match(df1, df2, allow_nullable_schema_mismatch) is True:
    print('Schema match.')
    if check_content_match(df1, df2, join_columns) is True:
      print('Content match.')
      return True
    else:
      print('Content mismatch.')
      return False
  else:
    print('Schema mismatch.')
    return False