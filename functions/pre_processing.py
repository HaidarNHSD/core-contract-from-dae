# Databricks notebook source
def get_gmspmstable() -> pd.DataFrame:
    """
    Get gms_pms_table. 
    Returns: gms_pms_table - dataframe contains necessary data
    """
    # Get database table from core_contract_cc_store
    gms_pms_table = spark.table('prim_core_contract.core_contract_cc_store')
    
    return gms_pms_table
  
  
def get_pracdf() -> pd.DataFrame:
    """
    Get practice_details_df. 
    Returns: practice_details_df - dataframe contains details on practice details
    """
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
    
    return practice_details_df