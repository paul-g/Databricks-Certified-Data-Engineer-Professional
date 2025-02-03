# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- add constraints on orders_silver to check timestamp > 2000 and quantity is > 0

# COMMAND ----------

# write a pyspark query to pull only 'orders'  with quantity > 0 from the bronze table


# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo_pro/checkpoints/orders_silver", True)