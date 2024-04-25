from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, row_number, max as spark_max, col, when, isnull
from pyspark.sql.window import Window

# Create SparkSession
spark = SparkSession.builder \
    .appName("CDC Example") \
    .getOrCreate()
    
# Input Data
data1 = [("AMCA2",5,20,20,'23-04-2024')]
data2 = [("AMCA1",6,30,35,'24-04-2024'),("AMCA2",5,20,21,'24-04-2024')]

# Create DataFrames
columns1 = ["Vessel_Name", "Height", "Weight", "Length", "Start_date"]
columns2 = ["Vessel_Name", "Height", "Weight", "Length", "Last_modified_date"]
df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)

# Convert 'Start date' column to date type
df1 = df1.withColumn('Start_date',to_date("Start_date", "dd-MM-yyyy"))
df2 = df2.withColumn('Last_modified_date', to_date("Last_modified_date", "dd-MM-yyyy"))
df1.show()
df2.show()

# Add End Date column
df1 = df1.withColumn('End Date', lit('9999-01-01'))
df2 = df2.withColumn('End Date', lit('9999-01-01'))
df1.show()
df2.show()

# Find new rows (inserts)
new_rows = df2.join(df1, "Vessel_Name", "leftanti").withColumnRenamed('Last_modified_date', 'Start_date')
new_rows.show()

# Find deleted rows
deleted_rows = df1.subtract(df2)
deleted_rows.show()

# Find updated rows (based on 'Vessel Name' which is assumed to be the key)
updated_rows = df1.join(df2, on="Vessel_Name", how="inner").filter(
    (df1["Height"] != df2["Height"]) |
    (df1["Weight"] != df2["Weight"]) |
    (df1["Length"] != df2["Length"]) |
    (df1["Start_date"] != df2["Last_modified_date"])
)#.withColumnRenamed('Last_modified_date', 'Start_date')
updated_rows.show()
updated_rows = updated_rows.select(df2["Vessel_Name"], df2["Height"], df2["Weight"], df2["Length"], df1["Start_date"],df2['Last_modified_date'].alias("End Date"))
updated_rows.show()

new_rows.show()
updated_rows.show()
deleted_rows.show()

final_df = new_rows.union(updated_rows).union(deleted_rows)
final_df.show()