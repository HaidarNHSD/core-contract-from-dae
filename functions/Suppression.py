# Databricks notebook source
def table_setup(final_df : DataFrame) -> DataFrame:
    """
    Explicity cast columns, ignore newer data and add index to main table and extract measures.
    
    Parameters:
        final_df: Processed Spark DataFrame.
        
    Returns:
        main_table: Spark DataFrame that has been processed for suppression.
      
    """
  
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
    
    return main_table

# COMMAND ----------

def pca_counts(main_table: DataFrame) -> DataFrame:
  
    """
    Add a PCA count column to main_table
    
    Parameters:
        main_table: Spark DataFrame that has been processed for suppression.
        
    Return:
        main_pca: Converted DataFrame 
    """  

    
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

    #display(main_pca)
    
    return main_pca
  

# COMMAND ----------

def suppression_rule_1(final_df : DataFrame) -> DataFrame:
    """
    Apply suppression rule 1 : For all fractional indicators: omit exclusion counts from the publication.
    
    Parameters:
        final_df: Processed Spark DataFrame.
        
    Return:
        main_pca: main_pca with suppression rule 1 applied
    """
    
    main_table = table_setup(final_df)
    main_pca = pca_counts(main_table)
    
    main_pca = (main_pca
        .filter(
            ~(F.col("MEASURE_TYPE")=="Exclusion")
        )
    )
    
    return main_pca, main_table

# COMMAND ----------

def suppression_rule_2(main_pca: DataFrame) -> DataFrame:
  
    """
    Apply suppression rule 2 : For fractional indicators with 1 PCA specified: where denominator <2 and PCA > 0, suppress the PCA and the denominator for that indicator.
    
    Parameters:
        main_pca: main_pca with suppression rule 1 applied
        
    Return:
        main_pca_1: Spark DataFrame with suppression rule 3 applied. 
    """  
    
    # Apply suppression rule 2, PCA = 1 & Denom < 2. Getting rows with denom < 2
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

    # Getting rows that meet Suppression Rule 2 by inner join of rows matching Denom < 2 condition and PCA value>0 condition
    pca_1_rule = (matches_denom_cond
        .join(
            matches_pca_value_cond,
            on = "PRAC_IND_CODE_ACH_DATE",
            how="inner"
        )
    )


    # Match records and add suppression indicator column, right join selects only matching records from main table 
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

    return main_pca_1

# COMMAND ----------

def suppression_rule_3(main_pca: DataFrame) -> DataFrame:
  
    """
    Apply suppression rule 3 : For fractional indicators with >1 PCA specified: where denominator = 0 and the sum of all PCAs is equal to the value of any one PCA, suppress all the PCAs for that indicator.
    
    Parameters:
        main_pca: main_pca with suppression rule 1 applied.
        
    Return:
        main_pca_2: Spark DataFrame with suppression rule 3 applied.
    """    
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

    #display(pca_2_total)

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
    #display(pca_2_supp_denom)

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

    pca_2_value = pca_2_value.withColumnRenamed("MEASURE_TYPE","MEASURE_TYPE2")
    #display(pca_2_value)

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
              how = "right"
        )
        .filter(
            (F.col("MEASURE_TYPE").isin(["PCA"]))
        )
        .withColumn("SUPPRESS", F.lit(1))
    )

    main_pca_2 = main_pca_2.drop(*["MEASURE_TYPE2"])
    return main_pca_2

# COMMAND ----------

def apply_suppression(final_df : DataFrame) -> None:
  
    """
    Combine all suppression together and display.
    
    Parameters:
        final_df: Processed Spark DataFrame.
        
    Return:
        main_table: Spark DataFrame that has been processed for suppression.
        main_supp_out : Suppressed Spark DataFrame
    """   
  
    main_pca, main_table = suppression_rule_1(final_df)
    main_pca_1 = suppression_rule_2(main_pca)
    main_pca_2 = suppression_rule_3(main_pca)
    
    # Form long skinny table of all suppressed records
    main_all_supp_matches =main_pca_1.union(main_pca_2)

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
    
    display(main_supp_out)
    
    return main_table, main_supp_out