# Databricks notebook source
def quartely_split(df : DataFrame, split_quarters = False) -> None:
    """
    Splits the DataFrame into four quarters.
    Parameters: 
    df - DataFrame to split into four quarters
    Returns:
    None
    """
    #This splits the data if enabled
    if split_quarters == True:
      
         quarter1 = main_supp_out.filter(
             (F.col('ACH_DATE') >= (report_period_start)) &
             (F.col('ACH_DATE') <= (report_period_end - pd.DateOffset(months=9)))
         )
        
         quarter2 = main_supp_out.filter(
             (F.col('ACH_DATE') >= (report_period_start + pd.DateOffset(months=3))) &
             (F.col('ACH_DATE') <= (report_period_end - pd.DateOffset(months=6)))
         )
          
         quarter3 =  main_supp_out.filter(
             (F.col('ACH_DATE') >= (report_period_start + pd.DateOffset(months=6))) &
             (F.col('ACH_DATE') <= (report_period_end - pd.DateOffset(months=3)))
         
         )
        
         quarter4 = main_supp_out.filter(
             (F.col('ACH_DATE') >= (report_period_start + pd.DateOffset(months=9))) & 
             (F.col('ACH_DATE') <= (report_period_end))
         )
        
         #Display each quarter
         display(quarter1)
         display(quarter2)
         display(quarter3)
         display(quarter4)

          
