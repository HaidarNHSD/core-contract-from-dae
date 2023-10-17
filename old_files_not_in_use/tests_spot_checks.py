# Databricks notebook source
# MAGIC %md
# MAGIC # NOTE : UNUSED FILE

# COMMAND ----------

### check removing Exclusions
def check_exclusion(depth = 3):
    """
    check exclusions have been removed
    """
    if depth <= 0:
        print("Failure in checking exclusion, no exclusion found in 3 randomised runs")
        return
      
    before_suppression_test_df, after_suppresion_test_df = spot_check(main_table, main_supp_out)
    exclusion_count_before = before_suppression_test_df.filter(col("MEASURE") == "EXCLUSION").count()

    if exclusion_count_before == 0:
        print("No exclusion examples found, randomising data again...")
        return check_exclusion(depth - 1)

    exclusion_count_after = after_suppression_test_df.filter(col("MEASURE") == "EXCLUSION").count()

    try: 
        assert exclusion_count_after == 0
    except AssertionError as e:
        print("Spot Check failure: Exlusions were not removed")
    

# COMMAND ----------

### check_suppression
def run_spot_check(depth = 3):
    """
    check suppression have been done correctly
    """
    if depth <= 0:
        print("Failure in checking suppression, no suppression found in 3 randomised runs")
        return
      
    before_suppression_df, after_suppression_df = spot_check(main_table, main_supp_out)
    
    suppression_count = after_suppression_df.filter(main_supp_out["VALUE"] == "*").count()

    if suppression_count == 0:
        print("No suppression examples found, randomising data again...")
        return check_suppression(depth - 1)

    
    #get the data where value = *
    filtered_data = after_suppression_df.filter(col("VALUE").contains("*"))
    
    #first practice and indicator thats filtered
    sup_practice = filtered_data.select("PRACTICE_CODE").distinct()[0]
    sup_indicator = filtered_data.select("IND_CODE").distinct()[0]
    
    #get the dataframe for only that practice
    supp_df = before_suppression_df.filter((col("PRACTICE_CODE") == sup_practice) & (col("IND_CODE") == sup_indicator))
    
    ##Count number of PCA's in this one practice
    pca_count = supp_df.filter(col("MEASURE") == "PCA").count()
    
    
    ###Rule 2:  for PCA count = 1 and denom < 2
    if pca_count == 1:
        denom_value = supp_df.filter(col("MEASURE") == "DENOMINATOR").select("VALUE")
        try: 
            assert denom_value < 2
            print("Spot Check: Suppression Rule 2 check SUCCESS")
        except AssertionError as e:
            print("Spot Check failure: Suppression Rule 2 check failed! please see the suppression for the following practice: ", sup_practice, ", and following indicator: ", sup_indicator)
            
            
    ### Rule 3 : For PCA count > 1 and denom < 2
    if pca_count > 1:
        denom_values = supp_df.filter(col("MEASURE") == "DENOMINATOR").select("VALUE")
        sup_count = 0
        for denom_value in denom_values:
            if denom_value == 0:
              sup_count += 1
        try: 
            assert sup_count > 0
            print("Spot Check: Suppression Rule 3 check SUCCESS")
        except AssertionError as e:
            print("Spot Check failure: Suppression Rule 3 check failed! please see the suppression for the following practice: ", sup_practice, ", and following indicator: ", sup_indicator)     
        