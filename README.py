# Databricks notebook source
# MAGIC %md
# MAGIC ## ALCOHOL AND GMS/PMS PUBLICATION JOB
# MAGIC This job outputs four quarterly CSVs for the entire financial year after suppression applied 
# MAGIC 
# MAGIC ## How to run
# MAGIC The main file to run is 'main.ipynb'. The code contains databrick widgets to display report times.
# MAGIC 
# MAGIC ## Installations
# MAGIC The required python packages to run this publication job are:  pyspark, pandas, unittests, typing & functools. 
# MAGIC 
# MAGIC ## Parameters
# MAGIC The file "parameters.ipynb" contains date parameters which can be changed to fit the dates required for your personalised report. 
# MAGIC 
# MAGIC ## Functions folder
# MAGIC The functions folder contains various functions used in the job which have been conveniently split into different sections. The "run_functions.ipynb" runs all the files in the functions in the functions file. This is done for ease of access in the "main.ipynb" file.
# MAGIC 
# MAGIC ## Tests folder
# MAGIC The tests folder contains various normal tests and unittests for each function of the code to check whether they are functioning correctly.
# MAGIC 
# MAGIC ## Spot checking
# MAGIC As the code is throughly checked and tested, the spot checking part of the code is done to manually assess the output for any errors the job did not catch.
# MAGIC 
# MAGIC ## Contact details
# MAGIC Contact Details		
# MAGIC Author: Primary Care Domain, NHS England		
# MAGIC Lead Analyst Rob Danks		
# MAGIC Public Enquiries: Telephone: 0300 303 5678		
# MAGIC Email: enquiries@nhsdigital.nhs.uk		
# MAGIC Press enquiries should be made to: Media Relations Manager: Telephone: 0300 303 3888		
# MAGIC 
# MAGIC ## Copyright
# MAGIC Published by NHS England, part of the Government Statistical Service		
# MAGIC Copyright © 2023, NHS England.				
# MAGIC You may re-use this document/publication (not including logos) free of charge in any format or medium, under the  terms of the		
# MAGIC Open Government Licence v3.0		
# MAGIC or write to the Information Policy Team, The National Archives,		
# MAGIC Kew, Richmond, Surrey, TW9 4DU;		
# MAGIC or email: psi@nationalarchives.gsi.gov.uk		