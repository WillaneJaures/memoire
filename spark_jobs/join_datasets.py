from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import os
import sys

spark = SparkSession.builder \
    .appName("Join Coinmarket & ExpatDakar Cleaned Datasets") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-9.3.0.jar") \
    .getOrCreate()

def resolve_single_csv_path(directory_path: str) -> str:
    if os.path.isdir(directory_path):
        csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
        if not csv_files:
            print(f"❌ No CSV files found in directory: {directory_path}")
            spark.stop()
            sys.exit(1)
        return os.path.join(directory_path, csv_files[0])
    return directory_path

# Inputs: cleaned single CSV directories
coinmarket_single_dir = "/tmp/cleaned_data_single"
expat_single_dir = "/tmp/expatdakar_cleaned_data_single"

coinmarket_csv = resolve_single_csv_path(coinmarket_single_dir)
expat_csv = resolve_single_csv_path(expat_single_dir)

print(f"Coinmarket cleaned CSV: {coinmarket_csv}")
print(f"ExpatDakar cleaned CSV: {expat_csv}")

df_coin = spark.read.csv(coinmarket_csv, header=True, inferSchema=True)

df_expat = spark.read.csv(expat_csv, header=True, inferSchema=True)

# Standardize schemas
# Coinmarket columns (lowercased): price, type, nombre_de_salle_bain, superficie, category, area, city, nombre_chambres
df_coin_std = df_coin \
    .withColumnRenamed("nombre_de_salle_bain", "nombre_sdb") \
    .withColumn("source", lit("coinmarket"))

# Expat columns: prix, type, nombre_sdb, superficie, category, nombre_chambres, quartier, region
df_expat_std = df_expat \
    .withColumnRenamed("prix", "price") \
    .withColumn("area", col("quartier")) \
    .withColumn("city", col("region")) \
    .withColumn("source", lit("expatdakar"))

# Ensure required columns exist on both (allowMissingColumns=True in union)
required_columns = [
    "price", "type", "superficie", "nombre_chambres", "nombre_sdb",
    "category", "area", "city", "source"
]

for c in required_columns:
    if c not in df_coin_std.columns:
        df_coin_std = df_coin_std.withColumn(c, lit(None))
    if c not in df_expat_std.columns:
        df_expat_std = df_expat_std.withColumn(c, lit(None))

df_coin_std = df_coin_std.select(required_columns)
df_expat_std = df_expat_std.select(required_columns)

df_joined = df_coin_std.unionByName(df_expat_std, allowMissingColumns=True)

print("✅ Preview of joined dataset:")
df_joined.show(20, truncate=False)

output_dir = "/tmp/joined_cleaned_data"
output_single = "/tmp/joined_cleaned_data_single"

df_joined.write.mode("overwrite").csv(output_dir, header=True)
df_joined.coalesce(1).write.mode("overwrite").option("header", True).csv(output_single)

print(f"✅ Joined dataset saved to: {output_dir}")
print(f"✅ Single CSV for joined dataset saved to: {output_single}")

spark.stop()


