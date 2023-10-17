# Databricks notebook source
#%run ./function_test_suite

# COMMAND ----------

#%run ../functions/run_functions

# COMMAND ----------

#Updated this section so now no inputs are in functions, but are localised here by one input
input_final_df = spark.createDataFrame(
      [
        ('Core GP Contract 2023-24', 'A81001', 'THE DENSHAM SURGERY', '2023-05-31', 'ALCMI30', 'Exclusion Count (Rule 2: FASTHIGH_DAT, AUDITCHIGH_DAT)', 'Exclusion', 123),
        ('Core GP Contract 2023-24', 'C81001', 'THE FAKE SURGERY 2', '2023-05-31', 'CCDCMI36', 'Exclusion Count (Rule 1: REG_DAT, PAT1_AGE)', 'Exclusion', 200),
        ('Core GP Contract 2023-24', 'A81001', 'THE DENSHAM SURGERY', '2023-04-30', 'ALCMI30', 'Numerator', 'Numerator', 134),
        ('Core GP Contract 2023-24', 'A81001', 'THE DENSHAM SURGERY', '2023-04-30', 'ALCMI30', 'Denominator', 'Denominator', 0),
        ('Core GP Contract 2023-24', 'A81001', 'THE DENSHAM SURGERY', '2023-04-30', 'ALCMI30', 'PCA Count (Rule 3: FRAILASSDEC_DAT)', 'PCA', 1),
        ('Core GP Contract 2023-24', 'C81005', 'THE FAKE SURGERY 1', '2023-05-31', 'CCDCMI36', 'Denominator', 'Denominator', 99),
        ('Core GP Contract 2023-24', 'DG3001', 'THE FAKE SURGERY 3', '2023-05-31', 'CCDCMI37', 'Denominator', 'Denominator', 0),
        ('Core GP Contract 2023-24', 'DG3001', 'THE FAKE SURGERY 3', '2023-05-31', 'CCDCMI37', 'PCA Count (Rule 3: FRAILASSDEC_DAT)', 'PCA', 1),
        ('Core GP Contract 2023-24', 'DG3001', 'THE FAKE SURGERY 3', '2023-05-31', 'CCDCMI37', 'PCA Count (Rule 3: FRAILASSDEC_DAT)', 'PCA', 0)
      ],
      ['QUALITY_SERVICE','PRACTICE_CODE','PRACTICE_NAME','ACH_DATE','IND_CODE','MEASURE', 'MEASURE_TYPE', 'VALUE']
    ) 

# COMMAND ----------

test_suite_instance = FunctionTestSuite()
@FunctionTestSuite.add_test
def table_setup_test() -> None:
    """
    Check that the table_Setup function works.
    """
    
    output_main_table = spark.createDataFrame(
        [
          ('Core GP Contract 2023-24','A81001','THE DENSHAM SURGERY','2023-04-30','ALCMI30','PCA Count (Rule 3: FRAILASSDEC_DAT)','PCA',1,'A81001_ALCMI30_2023-04-30'),
          ('Core GP Contract 2023-24','A81001','THE DENSHAM SURGERY','2023-04-30','ALCMI30','Denominator','Denominator',0,'A81001_ALCMI30_2023-04-30'),
          ('Core GP Contract 2023-24','A81001','THE DENSHAM SURGERY','2023-04-30','ALCMI30','Numerator','Numerator',134,'A81001_ALCMI30_2023-04-30'),
          ('Core GP Contract 2023-24','A81001','THE DENSHAM SURGERY','2023-05-31','ALCMI30','Exclusion Count (Rule 2: FASTHIGH_DAT, AUDITCHIGH_DAT)','Exclusion',123, 'A81001_ALCMI30_2023-05-31'),
          ('Core GP Contract 2023-24','C81001','THE FAKE SURGERY 2','2023-05-31','CCDCMI36','Exclusion Count (Rule 1: REG_DAT, PAT1_AGE)','Exclusion',200,'C81001_CCDCMI36_2023-05-31'),
          ('Core GP Contract 2023-24','C81005','THE FAKE SURGERY 1','2023-05-31','CCDCMI36','Denominator','Denominator',99,'C81005_CCDCMI36_2023-05-31'),
          ('Core GP Contract 2023-24','DG3001','THE FAKE SURGERY 3','2023-05-31','CCDCMI37','Denominator','Denominator',0,'DG3001_CCDCMI37_2023-05-31'),
          ('Core GP Contract 2023-24','DG3001','THE FAKE SURGERY 3','2023-05-31','CCDCMI37','PCA Count (Rule 3: FRAILASSDEC_DAT)','PCA',1,'DG3001_CCDCMI37_2023-05-31'),
          ('Core GP Contract 2023-24','DG3001','THE FAKE SURGERY 3','2023-05-31','CCDCMI37','PCA Count (Rule 3: FRAILASSDEC_DAT)','PCA',0,'DG3001_CCDCMI37_2023-05-31')
          ],
          ['QUALITY_SERVICE','PRACTICE_CODE','PRACTICE_NAME','ACH_DATE','IND_CODE','MEASURE', 'MEASURE_TYPE', 'VALUE','PRAC_IND_CODE_ACH_DATE']
        ) 

    output_main_table = output_main_table.withColumn("ACH_DATE", output_main_table["ACH_DATE"].cast("DATE"))
    output_main_table = output_main_table.withColumn("VALUE", output_main_table["VALUE"].cast("INT"))
    main_table = table_setup(input_final_df)
    
    try: 
        assert compare_results(output_main_table, main_table, join_columns = ['QUALITY_SERVICE','PRACTICE_CODE','PRACTICE_NAME','ACH_DATE','IND_CODE','MEASURE', 'MEASURE_TYPE',
                                                                              'VALUE','PRAC_IND_CODE_ACH_DATE'])
        log_test("UNIT TEST PASSED: " + 'table_setup_test')
    except AssertionError:
        log_test("UNIT TEST FAILURE: " + 'table_setup_test')

# COMMAND ----------

@FunctionTestSuite.add_test   
def pca_counts_test() -> None:
    """
    Check that the pca_counts function works
    """
    
    input_main_table = table_setup(input_final_df)

    input_main_table = input_main_table.withColumn("ACH_DATE", input_main_table["ACH_DATE"].cast("DATE"))
    input_main_table = input_main_table.withColumn("VALUE", input_main_table["VALUE"].cast("INT"))
    
    output_main_pca = spark.createDataFrame(
    [
      ('DG3001_CCDCMI37_2023-05-31', 'Core GP Contract 2023-24','DG3001','THE FAKE SURGERY 3','2023-05-31','CCDCMI37','PCA Count (Rule 3: FRAILASSDEC_DAT)','PCA',0,2),
      ('DG3001_CCDCMI37_2023-05-31', 'Core GP Contract 2023-24','DG3001','THE FAKE SURGERY 3','2023-05-31','CCDCMI37','Denominator','Denominator',0,2),
      ('DG3001_CCDCMI37_2023-05-31', 'Core GP Contract 2023-24','DG3001','THE FAKE SURGERY 3','2023-05-31','CCDCMI37','PCA Count (Rule 3: FRAILASSDEC_DAT)','PCA',1,2),
      ('C81005_CCDCMI36_2023-05-31', 'Core GP Contract 2023-24','C81005','THE FAKE SURGERY 1','2023-05-31','CCDCMI36','Denominator','Denominator',99,0),
      ('C81001_CCDCMI36_2023-05-31', 'Core GP Contract 2023-24','C81001','THE FAKE SURGERY 2','2023-05-31','CCDCMI36','Exclusion Count (Rule 1: REG_DAT, PAT1_AGE)','Exclusion',200,0),
      ('A81001_ALCMI30_2023-05-31', 'Core GP Contract 2023-24','A81001','THE DENSHAM SURGERY','2023-05-31','ALCMI30','Exclusion Count (Rule 2: FASTHIGH_DAT, AUDITCHIGH_DAT)','Exclusion',123,0),
      ('A81001_ALCMI30_2023-04-30', 'Core GP Contract 2023-24','A81001','THE DENSHAM SURGERY','2023-04-30','ALCMI30','PCA Count (Rule 3: FRAILASSDEC_DAT)','PCA',1,1),
      ('A81001_ALCMI30_2023-04-30', 'Core GP Contract 2023-24','A81001','THE DENSHAM SURGERY','2023-04-30','ALCMI30','Numerator','Numerator',134,1),
      ('A81001_ALCMI30_2023-04-30', 'Core GP Contract 2023-24','A81001','THE DENSHAM SURGERY','2023-04-30','ALCMI30','Denominator','Denominator',0,1)
      ],
      ['PRAC_IND_CODE_ACH_DATE','QUALITY_SERVICE','PRACTICE_CODE','PRACTICE_NAME','ACH_DATE','IND_CODE','MEASURE', 'MEASURE_TYPE', 'VALUE', 'PCA_COUNT']
    ) 

    output_main_pca = output_main_pca.withColumn("ACH_DATE", output_main_pca["ACH_DATE"].cast("DATE"))
    output_main_pca = output_main_pca.withColumn("VALUE", output_main_pca["VALUE"].cast("INT"))
    main_pca = pca_counts(input_main_table)

    try:
        assert compare_results(output_main_pca, main_pca, join_columns = ['PRAC_IND_CODE_ACH_DATE','QUALITY_SERVICE','PRACTICE_CODE','PRACTICE_NAME','ACH_DATE','IND_CODE','MEASURE', 
                                                                          'MEASURE_TYPE', 'VALUE', 'PCA_COUNT'])
        log_test("UNIT TEST PASSED: " + 'pca_counts_test')
    except AssertionError:
        log_test("UNIT TEST FAILURE: " + 'pca_counts_test')    

# COMMAND ----------

@FunctionTestSuite.add_test
def suppression_rule_1_test() -> None:
    """
    Check that it removes rows with Exclusion Count in the "MEASURE" column
    """
    
    output_main_pca = spark.createDataFrame(
      [
        ('DG3001_CCDCMI37_2023-05-31', 'Core GP Contract 2023-24', 'DG3001', 'THE FAKE SURGERY 3', '2023-05-31', 'CCDCMI37', 'PCA Count (Rule 3: FRAILASSDEC_DAT)', 'PCA', 0,2),
        ('DG3001_CCDCMI37_2023-05-31', 'Core GP Contract 2023-24', 'DG3001', 'THE FAKE SURGERY 3', '2023-05-31', 'CCDCMI37', 'PCA Count (Rule 3: FRAILASSDEC_DAT)', 'PCA', 1,2),
        ('DG3001_CCDCMI37_2023-05-31', 'Core GP Contract 2023-24', 'DG3001', 'THE FAKE SURGERY 3', '2023-05-31', 'CCDCMI37', 'Denominator', 'Denominator', 0, 2),
        ('C81005_CCDCMI36_2023-05-31', 'Core GP Contract 2023-24', 'C81005', 'THE FAKE SURGERY 1', '2023-05-31', 'CCDCMI36', 'Denominator', 'Denominator', 99, 0),
        ('A81001_ALCMI30_2023-04-30', 'Core GP Contract 2023-24', 'A81001', 'THE DENSHAM SURGERY', '2023-04-30', 'ALCMI30', 'Denominator', 'Denominator', 0, 1),
        ('A81001_ALCMI30_2023-04-30', 'Core GP Contract 2023-24', 'A81001', 'THE DENSHAM SURGERY', '2023-04-30', 'ALCMI30', 'PCA Count (Rule 3: FRAILASSDEC_DAT)', 'PCA', 1, 1), 
        ('A81001_ALCMI30_2023-04-30', 'Core GP Contract 2023-24', 'A81001', 'THE DENSHAM SURGERY', '2023-04-30', 'ALCMI30', 'Numerator', 'Numerator', 134, 1)
      ],
      ['PRAC_IND_CODE_ACH_DATE','QUALITY_SERVICE','PRACTICE_CODE','PRACTICE_NAME','ACH_DATE','IND_CODE','MEASURE', 'MEASURE_TYPE', 'VALUE', 'PCA_COUNT']
    ) 

    output_main_pca = output_main_pca.withColumn("ACH_DATE", output_main_pca["ACH_DATE"].cast("DATE"))
    output_main_pca = output_main_pca.withColumn("VALUE", output_main_pca["VALUE"].cast("INT"))
    #Should return empty dataframe
    main_pca, main_table = suppression_rule_1(input_final_df)
    
    #check if the output row is empty 
    try:
        assert compare_results(output_main_pca, main_pca, join_columns = ['PRAC_IND_CODE_ACH_DATE','QUALITY_SERVICE','PRACTICE_CODE','PRACTICE_NAME','ACH_DATE','IND_CODE',
                                                                          'MEASURE', 'MEASURE_TYPE', 'VALUE', 'PCA_COUNT'])
        log_test("UNIT TEST PASSED: " + 'suppression_rule_1_test')
    except AssertionError:
        log_test("UNIT TEST FAILURE: " + 'suppression_rule_1_test')  

# COMMAND ----------

#Obtain the input dataframe for the next sections
input_main_pca, main_table = suppression_rule_1(input_final_df)

# COMMAND ----------

@FunctionTestSuite.add_test
def suppression_rule_2_test() -> None:
    """
    Check if suppression rule 2 works
    """
    output_main_pca_1 = spark.createDataFrame(
      [
        ('A81001_ALCMI30_2023-04-30', 'PCA', 1),
        ('A81001_ALCMI30_2023-04-30','Denominator', 1)
      ],
      ['PRAC_IND_CODE_ACH_DATE','MEASURE_TYPE', 'SUPPRESS']
    ) 
     
    output_main_pca_1 = output_main_pca_1.withColumn("SUPPRESS", output_main_pca_1["SUPPRESS"].cast("INT"))
    main_pca_1 = suppression_rule_2(input_main_pca)
  
    try:
        assert compare_results(output_main_pca_1, main_pca_1, join_columns = ['PRAC_IND_CODE_ACH_DATE','MEASURE_TYPE', 'SUPPRESS'])
        log_test("UNIT TEST PASSED: " + 'suppression_rule_2_test')
    except AssertionError:
        log_test("UNIT TESTFAILURE: " + 'suppression_rule_2_test')  

# COMMAND ----------

@FunctionTestSuite.add_test
def suppression_rule_3_test() -> None:
    """
    Check if suppression rule 3 works
    """
    output_main_pca_2 = spark.createDataFrame(
      [
        ('DG3001_CCDCMI37_2023-05-31', 'PCA', 1),
        ('DG3001_CCDCMI37_2023-05-31', 'PCA', 1)
      ],
      ['PRAC_IND_CODE_ACH_DATE','MEASURE_TYPE', 'SUPPRESS']
    ) 

    output_main_pca_2 = output_main_pca_2.withColumn("SUPPRESS", output_main_pca_2["SUPPRESS"].cast("INT"))
    main_pca_2 = suppression_rule_3(input_main_pca)

    try:
        assert compare_results(output_main_pca_2, main_pca_2, join_columns = ['PRAC_IND_CODE_ACH_DATE','MEASURE_TYPE', 'SUPPRESS'])
        log_test("UNIT TEST PASSED: " + 'suppression_rule_3_test')
    except AssertionError:
        log_test("UNIT TEST FAILURE: " + 'suppression_rule_3_test')  

# COMMAND ----------

if __name__ == '__main__':
    test_suite_instance.run()
    #unittest.main(argv=['first-arg-is-ignored'], exit =False)  