# Databricks notebook source
# MAGIC %md
# MAGIC ## Fanbase Engagement EDA
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import avg, col, desc, count, when, round, sum


# COMMAND ----------

fanbase = spark.table("workspace.default.fanbase_engagement")
display(fanbase)

# COMMAND ----------

fanbase.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning
# MAGIC No nulls found.

# COMMAND ----------

fanbase.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in fanbase.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis
# MAGIC - 18-25 age group has the most members
# MAGIC - 60+ age group has the least members

# COMMAND ----------

fanbase.filter(col("Customer_Region") == "Canada").groupBy("Age_Group").count().withColumnRenamed("count", "Members").display()

# COMMAND ----------

fanbase.count()
# fanbase.filter(col("Customer_Region") == "Canada").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Games Attended by age group
# MAGIC - Each age group attends roughly the same number of games on average

# COMMAND ----------

fanbase.groupBy("Age_Group").agg(avg("Games_Attended").alias("Average_Games_Attended")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Games Attended by seasonal pass holders across age groups

# COMMAND ----------

members_per_age_group = fanbase.filter(col("Customer_Region") != "Canada").groupby("Age_Group").agg(
  count("Membership_ID").alias("Total_Members"),
  sum(when(col("Seasonal_Pass") == True, 1).otherwise(0)).alias("Pass_Holders"))
table = members_per_age_group.withColumn(
    "Pass_Rate (%)",
    round(col("Pass_Holders") / col("Total_Members") * 100, 2)
)


display(table)
table.groupby().sum().collect()

# COMMAND ----------

fanbase.groupby(["Age_Group", "Seasonal_Pass"]).agg(
  avg("Games_Attended").alias("Avg_Games_Attended")
  ).withColumn("Seasonal_Pass", when(col("Seasonal_Pass") == True, "Yes").otherwise("No")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Games Attended and Seasonal pass holders by Locality

# COMMAND ----------

fanbase.groupby("Customer_Region").agg(count("Membership_ID")).display()

# COMMAND ----------

fanbase.groupby("Customer_Region").agg(
  sum(when(col("Seasonal_Pass") == True, 1).otherwise(0)).alias("Pass_Holders"),
  sum(when(col("Seasonal_Pass") == False, 1).otherwise(0)).alias("Non_Pass_Holders")
).withColumn("Season pass rate(%)", round(col("Pass_Holders") / (col("Pass_Holders") + col("Non_Pass_Holders")) * 100, 2)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC Increasing seasonal pass could increase game attendance by a considerable amount as on average, seasonal pass holders go to 5x more games than non-pass holders across all age groups. 
# MAGIC
# MAGIC This could be achieved by making seasonal pass tickets more attractive by offering exclusive discounts/offers to ticket holders. These offers could also be tailored to each age group. For example, 18-25 could get discounts on hoodies/jerseys as that's the best seller among that group. 60+ could get offers on more "accessible" seats / whatever old ppl like.
# MAGIC
# MAGIC Seasonal pass marketing should be targeted towards 18-25 and 26-40.
# MAGIC   - Most seasonal pass holders are these age groups -> pricing is optimal
# MAGIC   - most likely to have children -> emphasize family experiences/bundles in seasonal pass marketing.
# MAGIC     - e.g. cheaper youth tickets, exclusive meet and greets, football lessons
# MAGIC   - increase longevity of fanbase loyalty

# COMMAND ----------


