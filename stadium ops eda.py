# Databricks notebook source
# MAGIC %md
# MAGIC ## Stadium Ops EDA
# MAGIC Dataset is for 2024

# COMMAND ----------

from pyspark.sql.functions import sum, avg, col, desc, count, when, round, date_format, asc, mean

ops = spark.table("workspace.default.stadium_ops")
display(ops)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning
# MAGIC No null values.

# COMMAND ----------

from pyspark.sql.functions import create_map, lit, sum, date_format
from itertools import chain

ops.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in ops.columns
]).show()

ops.select("Source").distinct().show()

month_map = {
    1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun",
    7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"
}

mapping_expr = create_map([lit(x) for x in chain(*month_map.items())])

ops = ops.withColumn(
    "Month", mapping_expr[col("Month")]
)

display(ops)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis
# MAGIC - January, June, November, and December seem to be off season as Bolt FC operates at a loss during these months.
# MAGIC - The highest cost is staffing. Reducing staff is a potential solution, however, it may hinder the quality of services and reputation of the club.

# COMMAND ----------

ops.select("Source").distinct().show()

# COMMAND ----------

# Monthly Profit
ops.groupby("Month").agg(sum("Revenue")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyzing off-season months
# MAGIC - During off-season months, staffing costs result in significant losses. Try to reduce staffing costs.
# MAGIC   - For example, in December, closing uppe 
# MAGIC - Reduce staff during off-season? (e.g. close upper bowl since there is very little demand)
# MAGIC - Prioritize concerts/conferences during off-season. Food tracks closely + sponsorship opportunities.

# COMMAND ----------

for month in ["Jan", "Jun", "Nov", "Dec"]:
    ops.filter(col("Month") == month).display()

ops.filter(col("Month").isin(["Jan", "Jun", "Nov", "Dec"])).where(col("Source").isin(["Maintenance", "Upper Bowl"])).display()

# COMMAND ----------

ops.select('*').where(col("Source") == "Concert").where(col("Source") == "Conference").show()

# COMMAND ----------

ops.select("*").where(col("Source").isin(["Upper Bowl", "Lower Bowl", "Food"])).display()


# COMMAND ----------

for month in ["Aug", "Sep", "Oct"]:
    ops.filter(col("Month") == month).display()

# COMMAND ----------

pivot_df = (
    ops.filter(col("Source") != "Insurance").groupBy("Month")
      .pivot("Source")
      .agg(avg("Revenue"))
)

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Collect the pivoted DataFrame to pandas
pivot_pd = pivot_df.toPandas()


# Select only the numeric columns (excluding 'Month')
cols = [c for c in pivot_pd.columns if c != "Month"]

# Compute the correlation matrix using pandas
corr_df = pivot_pd[cols].corr(method="spearman")

mask = np.triu(np.ones_like(corr_df, dtype=bool),k=1)

display(corr_df)

# Create the heatmap
plt.figure(figsize=(8,6))
sns.heatmap(
    corr_df,
    mask=mask,
    annot=True,
    cmap="coolwarm",
    fmt=".2f",
    xticklabels=corr_df.columns,
    yticklabels=corr_df.columns
)

plt.title("Revenue (Spearman) Correlation Between Sources")
plt.xlabel("Source")
plt.ylabel("Source")
plt.tight_layout()
plt.show()

# COMMAND ----------

ops.select("*").where(col("Source").isin(["Upper Bowl", "Lower Bowl", "Food", "Season", "Premium"])).display()

# COMMAND ----------

ops.select("*").where(col("Source").isin(["Concert", "Utilities"])).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
