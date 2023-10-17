# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT SUM(VALUE)
# MAGIC From prim_core_contract.core_contract_dementia_store
# MAGIC where IND_CODE = 'DEMMI202' and measure = 'Numerator' and Ach_Date = '2023-08-31'