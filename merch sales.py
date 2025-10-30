# Databricks notebook source
# MAGIC %md
# MAGIC ## Merchandise Sales EDA

# COMMAND ----------

from pyspark.sql.functions import sum, avg, col, desc, count, when, round, date_format, asc, mean

sales = spark.table("workspace.default.merchandise_sales")
display(sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning
# MAGIC - Null arrival dates mean purchases were at the team store
# MAGIC - Null sizes are for merchandise with no sizes

# COMMAND ----------

sales = sales.fillna({"Size": "No size"})
display(sales)

# COMMAND ----------

sales.select("Item_Name").distinct().show()

# COMMAND ----------

sales.groupby(["Item_Name", "Size"]).agg()
