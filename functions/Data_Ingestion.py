# Databricks notebook source
def data_ingestion(print_tables = False):
    """
    Ingest and process the data.
    
    Parameters:
    
    Returns:
        final_df: Processed Spark DataFrame.
    """
    
    # Get database table from core_contract_cc_store
    gms_pms_table = get_gmspmstable()

    # Select required columns to be included in dataframe
    # Replace blank values in QUALITY_SERVICE  column with 'Core GP Contract 'service_year''
    gms_pms_df = gms_pms_table.select(
        'ORG_CODE', 
        F.when(col('QUALITY_SERVICE') == "", quality_service_name).otherwise(col('QUALITY_SERVICE')).alias('QUALITY_SERVICE'),
        'ACH_DATE',
        'IND_CODE',
        'MEASURE',
        'VALUE'  
    ).withColumnRenamed('ORG_CODE', 'PRACTICE_CODE'
    ).filter(F.col('ACH_DATE') >= report_start
    ).filter(F.col('ACH_DATE') <= report_end)
    # Create a new MEASURE_TYPE column that identifies the type of measure
    gms_pms_df = gms_pms_df.withColumn("MEASURE_TYPE", substring_index(gms_pms_df.MEASURE, ' ', 1))

    
    #Log the number of distinct rows and total rows in the DataFrame
    log_info("Distinct Rows Count:" + str(gms_pms_df.distinct().count()))
    log_info("Total Rows Count:" + str(gms_pms_df.count()))
    
    # Creating a new DataFrame "practice_details_df" containing practice code and practice name
    practice_details_df = get_pracdf()

    # Log the count of rows in "practice_details_df"
    log_info("Practice Details Rows Count:" + str(practice_details_df.count()))
    #Log the count of distinct rows in "practice_details_df"
    log_info("Practice Details Distinct Rows Count:" + str(practice_details_df.distinct().count()))
    
    # Joining "gms_pms_df" and "practice_details_df" with a left join
    final_df = gms_pms_df.join(
        practice_details_df , 
        on= (gms_pms_df['PRACTICE_CODE']== practice_details_df['CODE']), 
        how='left'
    ).withColumnRenamed('NAME','PRACTICE_NAME'
    ).drop('CODE'
    ).orderBy('PRACTICE_CODE','ACH_DATE','IND_CODE')
    
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
    if (print_tables):
        display(final_df)

    #Logging the count of the final DataFrame
    log_info("Count of final dataframe is " + str(final_df.count()))
    
    log_info("Ran Data_Ingestion Sucessfully")
    return final_df