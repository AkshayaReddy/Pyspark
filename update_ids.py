from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, row_number, max as spark_max, col, when, isnull
from pyspark.sql.window import Window

# Create SparkSession
spark = SparkSession.builder \
    .appName("Merge Tables Example") \
    .getOrCreate()

# Input Data
data1 = [("Akshaya", 1, "01-01-2024", 10),
         ("Shreya", 2, "01-01-2024", 10),
         ("Akshaya", 1, "01-02-2024", 15)]

data2 = [("Akshaya", 999, "22-04-2024", 25),
         ("Amith", 222, "22-04-2024", 30)]

# Create DataFrames
columns = ["Name", "ID", "Date", "Salary"]
df1 = spark.createDataFrame(data1, columns)
df2 = spark.createDataFrame(data2, columns)

# Add an additional column to each DataFrame to denote the source table
df1 = df1.withColumn("Table", lit("Table1"))
df2 = df2.withColumn("Table", lit("Table2"))

# Union the two DataFrames together
union_df = df1.union(df2)
union_df.show()

ids_from_table1 = df1.select("Name", col("ID").alias("Updated_ID")).distinct()
df_updated = union_df.join(ids_from_table1, on="Name", how="left")
max_id_table1 = union_df.filter(col("Table") == "Table1").agg(spark_max("ID")).collect()[0][0]
df_cht = df_updated.withColumn("Updated_ID", when((col("Table") == "Table2") & (~col("Name").isin(names_to_check)) & isnull(col("Updated_ID")), max_id_table1 + 1).otherwise(col("Updated_ID")))
df_cht.show()


# Define a window specification
window_spec = Window.partitionBy("Name").orderBy(col("Date").desc())

# Add a new column "row_num" to assign row numbers within each partition
df_with_row_num = df_cht.withColumn("row_num", row_number().over(window_spec))

# Filter the rows with row number 1 to get the most recent records for each Name
most_recent_records = df_with_row_num.filter(col("row_num") == 1)

# Select the desired columns
result = most_recent_records.select("Name", "ID", "Date", "Salary", "Table", "Updated_ID")

result.show()