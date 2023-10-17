# Databricks notebook source
# MAGIC %md
# MAGIC # ALCOHOL AND GMS/PMS PUBLICATION JOB
# MAGIC 
# MAGIC This job outputs four quarterly CSVs for the entire financial year after suppression applied 

# COMMAND ----------

# MAGIC %md
# MAGIC ###CONTENTS
# MAGIC <ol>
# MAGIC   <li>Getting data from the database</li>
# MAGIC   <li>Testing outputs</li>
# MAGIC   <li>Suppresssion</li>
# MAGIC   <li>Quarterly Split</li>
# MAGIC   <li>Spot checks on few practices</li>
# MAGIC   <li>Addendum : Suppresssion rule 3</li>
# MAGIC </ol>

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import substring_index,substring,count,countDistinct,col , when
import pandas as pd
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, LongType

# COMMAND ----------

#create widget for data report time
dbutils.widgets.text("report_end", defaultValue='2023-03-31', label="Reporting Period End Date")
dbutils.widgets.text("report_start", defaultValue='2022-04-01' , label="Reporting Period Start Date")
dbutils.widgets.text("service_year", defaultValue='2023-24', label="Service Year")

#Get values from widgets
report_start = dbutils.widgets.get("report_start")
report_period_start = pd.to_datetime(report_start)

report_end = dbutils.widgets.get("report_end")
report_period_end = pd.to_datetime(report_end)

service_year = dbutils.widgets.get("service_year")

#Define the quality service name based on the service year
quality_service_name = f"Core GP Contract {service_year}"

#Print the widget values for verification
print("Report Start:", report_period_start)
print("Report End:", report_period_end)
print("Quality Service Name:", quality_service_name)

# COMMAND ----------

gms_pms_table = spark.table('prim_core_contract.core_contract_cc_store')

unique_values = gms_pms_table.select("MEASURE").distinct().collect()
print(unique_values)

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA INGESTION

# COMMAND ----------

# Get database table from core_contract_cc_store
gms_pms_table = spark.table('prim_core_contract.core_contract_cc_store')

# Select required columns to be included in dataframe
# Replace blank values in QUALITY_SERVICE  column with 'Core GP Contract 'service_year''
gms_pms_df = gms_pms_table.select(
    'ORG_CODE', 
    F.when(col('QUALITY_SERVICE') == "", quality_service_name).otherwise(col('QUALITY_SERVICE')).alias('QUALITY_SERVICE'),
    'ACH_DATE',
    'IND_CODE',
    'MEASURE',
    'VALUE'  
).withColumnRenamed('ORG_CODE', 'PRACTICE_CODE').filter(F.col('ACH_DATE') >= report_start).filter(F.col('ACH_DATE') <= report_end)

# Create a new MEASURE_TYPE column that identifies the type of measure
gms_pms_df = gms_pms_df.withColumn("MEASURE_TYPE", substring_index(gms_pms_df.MEASURE, ' ', 1))

#Display the DataFrame
display(gms_pms_df)

#Print the numebr of distinct rows and total rows in the DataFrame
print("Distinct Rows Count:", gms_pms_df.distinct().count())
print("Total Rows Count:", gms_pms_df.count())

# COMMAND ----------

# Creating a new DataFrame "practice_details_df" containing practice code and practice name
practice_details_df = spark.sql(f"""
    SELECT CODE, NAME
    FROM dss_corporate.ods_practice_v02
    WHERE
        DSS_RECORD_START_DATE <= '{report_period_end}'
        AND (DSS_RECORD_END_DATE >= '{report_period_end}' OR DSS_RECORD_END_DATE IS NULL) 
        AND PRESCRIBING_SETTING = 4
    ORDER BY CODE
""")

# Displaying the count of rows in "practice_details_df"
print("Practice Details Rows Count:", practice_details_df.count())

#Displaying the count of distinct rows in "practice_details_df"
print("Practice Details Distinct Rows Count:", practice_details_df.distinct().count())

# COMMAND ----------

# Joining "gms_pms_df" and "practice_details_df" with a left join
final_df = gms_pms_df.join(
    practice_details_df , 
    on= (gms_pms_df['PRACTICE_CODE']== practice_details_df['CODE']), 
    how='left'
).withColumnRenamed('NAME','PRACTICE_NAME').drop('CODE').orderBy('PRACTICE_CODE','ACH_DATE','IND_CODE')

#Displaying the final DataFrame
display(final_df)

# COMMAND ----------

#The columns of "final_df" are ordered by PRACTICE_CODE, ACH_DATE and IND_CODE
final_df= final_df.select(
    'QUALITY_SERVICE',
    'PRACTICE_CODE',
    'PRACTICE_NAME',
    'ACH_DATE',
    'IND_CODE',
    'MEASURE', 
    'MEASURE_TYPE',
    'VALUE'
).orderBy('PRACTICE_CODE','ACH_DATE','IND_CODE')

#Displaying the final DataFrame
display(final_df)

#Displaying the count of the final DataFrame
print(final_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #TESTING

# COMMAND ----------

# Test 1: Number of unique practices in the source data
sourceNopractices = spark.sql(f"""
    SELECT COUNT(DISTINCT ORG_CODE) FROM  prim_core_contract.core_contract_cc_store
    WHERE  ACH_DATE >= '{report_period_start}'
    AND ACH_DATE <= '{report_period_end}'
""").first()['count(DISTINCT ORG_CODE)']


outputNoPractices = final_df.select('PRACTICE_CODE').distinct().count()
print('sourceNopractices : ', sourceNopractices)
print('outputNoPractices : ', outputNoPractices)

# COMMAND ----------

# Test 2: Number of output practices matches the number of source practices
check2 = sourceNopractices == outputNoPractices  
if check2:
  print(f'Matched with practice count in database = {sourceNopractices} practices') 
else:
  print(f'Did not Match with practice count in database') 

# COMMAND ----------

# Test 3: Same number of indicator codes per practice
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

# COMMAND ----------

# Combining all the three test outputs into a single dataframe for exporting
columns = ['TESTS', 'PASS/FAIL']
values  =[
  ('Number of unique practices  ' ,str(sourceNopractices)),
  ('Number of output practices matches the number of source practices', str(check2)),
  ('Same number of indicator codes per practice', str(check3)),
]
print('Number of unique practices' ,outputNoPractices)
test_df = spark.createDataFrame(values, columns)
display(test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #SUPPRESSION

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Table setup

# COMMAND ----------

# Explicitly cast columns and ignore April data
final_df_cast = (final_df
    .select(
        F.col("QUALITY_SERVICE").cast("STRING"),
        F.col("PRACTICE_CODE").cast("STRING"),
        F.col("PRACTICE_NAME").cast("STRING"),
        F.col("ACH_DATE").cast("DATE"),
        F.col("IND_CODE").cast("STRING"),
        F.col("MEASURE").cast("STRING"),
        F.col("MEASURE_TYPE").cast("STRING"),
        F.col("VALUE").cast("INT")
     )
     .filter(
         (F.col("ACH_DATE") <= report_end)
     )
     .orderBy('PRACTICE_CODE','ACH_DATE','IND_CODE')
)

# Add index to main table and extract measures
main_table = (final_df_cast
    .withColumn("PRAC_IND_CODE_ACH_DATE", 
     F.concat(F.col("PRACTICE_CODE"), 
     F.lit("_"), 
     F.col("IND_CODE"), 
     F.lit("_"), 
     F.col("ACH_DATE")))
)

display(main_table)

# COMMAND ----------

print(report_end)
print(type(report_end))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PCA counts

# COMMAND ----------

# Count number of PCAs for each practice and indicator combination
pca_total = (main_table
    .filter(
        (F.col("MEASURE_TYPE")=="PCA")
    )
    .groupBy(
        F.col("PRAC_IND_CODE_ACH_DATE")
    )
    .agg(F.count(F.col("MEASURE_TYPE")).alias("PCA_COUNT"))
)

# Join back to main table
main_pca = (main_table
    .join(
        pca_total,
        on = "PRAC_IND_CODE_ACH_DATE",
        how = "left"
     )
     .fillna(0, "PCA_COUNT")
)

display(main_pca)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Suppression rules

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Exclusions

# COMMAND ----------

# Apply suppression rule 1
# Omit exclusions
main_pca = (main_pca
    .filter(
        ~(F.col("MEASURE_TYPE")=="Exclusion")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### PCA = 1, Denom <2

# COMMAND ----------

main_pca.count()
print(main_pca.columns)

# COMMAND ----------

# Apply suppression rule 2
# PCA = 1 & Denom < 2
# Getting rows with denom < 2
matches_denom_cond = (main_pca
    .select(
        F.col("PRAC_IND_CODE_ACH_DATE")
    )
    .filter(
        (F.col("PCA_COUNT") == 1) &
        (F.col("MEASURE_TYPE") == "Denominator") &
        (F.col("VALUE") < 2)
    )
)

# COMMAND ----------

# Getting rows with value of PCA greater than 0
matches_pca_value_cond = (main_pca
    .select(
        F.col("PRAC_IND_CODE_ACH_DATE")
    )
    .filter(
        (F.col("PCA_COUNT") == 1) &
        (F.col("MEASURE_TYPE") == "PCA") &
        (F.col("VALUE") > 0)
    )
)

# COMMAND ----------

# Getting rows that meet Suppression Rule 2
# By inner join of rows matching Denom < 2 condition and PCA value>0 condition
pca_1_rule = (matches_denom_cond
    .join(
        matches_pca_value_cond,
        on = "PRAC_IND_CODE_ACH_DATE",
        how="inner"
    )
)

# COMMAND ----------

# Match records and add suppression indicator column
# Right join selects only matching records from main table 
pca_1_match = (main_pca
    .select(
        F.col("PRAC_IND_CODE_ACH_DATE"),
        F.col("MEASURE_TYPE")
    )
    .join(
        pca_1_rule,
        on = "PRAC_IND_CODE_ACH_DATE",
        how = "right"
     )
)

# COMMAND ----------

# Indicate suppression for PCA and Denominator measures
main_pca_1 = (pca_1_match
    .select(
        F.col("PRAC_IND_CODE_ACH_DATE"),
        F.col("MEASURE_TYPE")
    )
    .filter(
        F.col("MEASURE_TYPE").isin(["PCA", "Denominator"])
    )
    .withColumn("SUPPRESS", F.lit(1))
)

display(main_pca_1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### PCA > 1 suppression

# COMMAND ----------

# Calculate total value of PCAS for indicators with more than one PCA
pca_2_total = (main_pca
    .select(
        F.col("PRAC_IND_CODE_ACH_DATE"),
        F.col("VALUE")
    )
    .filter(
        (F.col("PCA_COUNT") > 1) &
        (F.col("MEASURE_TYPE") == "PCA")
    )
    .groupBy(
        F.col("PRAC_IND_CODE_ACH_DATE"),
    )
    .agg(F.sum(F.col("VALUE")).alias("PCA_VALUE_TOTAL"))
)

display(pca_2_total)

# COMMAND ----------

# Select records with denom = 0 but PCA total > 0
pca_2_supp_denom = (main_pca
    .join(
        pca_2_total,
        on ="PRAC_IND_CODE_ACH_DATE",
        how = "left"
    )
    .fillna(0, "PCA_VALUE_TOTAL")
    .filter(
        (F.col("MEASURE_TYPE")=="Denominator") &
        (F.col("VALUE") == 0) &
        (F.col("PCA_VALUE_TOTAL") > 0)
    )
)

# COMMAND ----------

# Indicate suppression for records with a single PCA value equal to the total of PCAs for that indicator
pca_2_value = (main_pca
    .select(
        F.col("PRAC_IND_CODE_ACH_DATE"),
        F.col("MEASURE_TYPE"),
        F.col("VALUE")
    )
    .join(
         pca_2_supp_denom.select(F.col("PRAC_IND_CODE_ACH_DATE"), F.col("PCA_VALUE_TOTAL")),
         on = "PRAC_IND_CODE_ACH_DATE",
         how = "right"
    )
    .filter(
          (F.col("MEASURE_TYPE") == "PCA") &
          (F.col("VALUE") == F.col("PCA_VALUE_TOTAL"))
    )
    .withColumn("SUPPRESS", F.lit(1))
)

print(pca_2_value.columns)
pca_2_value = pca_2_value.withColumnRenamed("MEASURE_TYPE","MEASURE_TYPE2")
display(pca_2_value)

# COMMAND ----------

# Select all PCAs for records matching suppression above
main_pca_2 = (main_pca
    .select(
        F.col("PRAC_IND_CODE_ACH_DATE"),
        F.col("MEASURE_TYPE"),
    )
    .join(
        pca_2_value
         .select(
             F.col("PRAC_IND_CODE_ACH_DATE"),
             F.col("MEASURE_TYPE2"),
              F.col("SUPPRESS")
          )
          .filter(
              (F.col("SUPPRESS") == 1)
          ),
          on = ["PRAC_IND_CODE_ACH_DATE"],
          how = "left"
    )
    .filter(
        (F.col("MEASURE_TYPE").isin(["PCA"]))
    )
    .withColumn("SUPPRESS", F.lit(1))
)

main_pca_2 = main_pca_2.drop(*["MEASURE_TYPE2"])
print(main_pca_2.columns)
display(main_pca_2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Apply suppression

# COMMAND ----------

# Form long skinny table of all suppressed records
main_all_supp_matches =main_pca_1.union(main_pca_2)     # Additional union to main_pca_2 would add other rule if required eg:  main_pca_1.union(main_pca_2) 
#main_all_supp_matches = main_pca_2

# Join back to main table and suppress
main_supp = (main_pca
    .join(
        main_all_supp_matches,
        on = ["PRAC_IND_CODE_ACH_DATE", "MEASURE_TYPE"],
        how = "left"
     )
     .withColumn(
         "VALUE",
          F.when(
          (F.col("SUPPRESS")==1),
          "*"
     )
     .otherwise(F.col("VALUE"))
     )
)

# Select output columns
main_supp_out = (main_supp
    .select(
        F.col("QUALITY_SERVICE"), 
        F.col("PRACTICE_CODE"),
        F.col("PRACTICE_NAME"), 
        F.col("ACH_DATE"),
        F.col("IND_CODE"),
        F.col("MEASURE"), 
        F.col("MEASURE_TYPE"), 
        F.col("VALUE")
    )
    .orderBy('PRACTICE_CODE','ACH_DATE','IND_CODE')
 )

# COMMAND ----------

# DBTITLE 1,Suppressed output for publication
display(main_supp_out)
#main_supp_out.count()
#prac_list = main_supp_out.filter(main_supp_out["VALUE"] == "*").select("PRACTICE_CODE").distinct().collect()
#prac_code_list = [item["PRACTICE_CODE"] for item in prac_list]
#main_supp_out2 = main_supp_out.filter(col("PRACTICE_CODE").isin(prac_code_list))
#display(main_supp_out2)

# COMMAND ----------

# MAGIC %md
# MAGIC #QUARTERLY SPLIT

# COMMAND ----------

# DBTITLE 1,Separating data into 4 quarters
# quarter1 = main_supp_out.filter(
#                                 (F.col('ACH_DATE') >= (report_period_start)) &
#                                 (F.col('ACH_DATE') <= (report_period_end - pd.DateOffset(months=9)))
#                                 )
# quarter2 = main_supp_out.filter(
#                                 (F.col('ACH_DATE') >= (report_period_start + pd.DateOffset(months=3))) &
#                                 (F.col('ACH_DATE') <= (report_period_end - pd.DateOffset(months=6)))
#                                 )
# quarter3 =  main_supp_out.filter(
#                                 (F.col('ACH_DATE') >= (report_period_start + pd.DateOffset(months=6))) &
#                                 (F.col('ACH_DATE') <= (report_period_end - pd.DateOffset(months=3)))
#                                 )
# quarter4 = main_supp_out.filter(
#                                 (F.col('ACH_DATE') >= (report_period_start + pd.DateOffset(months=9))) & 
#                                 (F.col('ACH_DATE') <= (report_period_end))
#                                 )

# COMMAND ----------

# display(quarter1)
# display(quarter2)
# display(quarter3)
# display(quarter4)

# COMMAND ----------

# Getting count of number of rows suppressed
# And count of number of practices suppressed
suppressed = main_supp_out.filter(main_supp_out.VALUE == '*')
display(suppressed)
display(suppressed.select('PRACTICE_CODE').distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC #SPOT CHECKS

# COMMAND ----------

# DBTITLE 1,Testing suppression on few practices
# Getting the original dataframe before suppression to export out for PRACTICE_CODE == 'A82006' and 'Y06007'
before_suppression_test_df = main_table.filter((main_table.PRACTICE_CODE == 'A82006')|(main_table.PRACTICE_CODE == 'Y06007')|(main_table.PRACTICE_CODE == 'Y07060')|(main_table.PRACTICE_CODE == 'N84626'))
display(before_suppression_test_df)

# COMMAND ----------

# Testing if the 3 rules of suppression are satisified in the output for PRACTICE_CODE == 'A82006' and 'Y06007'
after_suppression_test_df = main_supp_out.filter((main_table.PRACTICE_CODE == 'A82006')|(main_table.PRACTICE_CODE == 'Y06007')|(main_table.PRACTICE_CODE == 'Y07060')|(main_table.PRACTICE_CODE == 'N84626'))
display(after_suppression_test_df)