from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, lower
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
            print(f" No CSV files found in directory: {directory_path}")
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

# Ensure required columns exist on both
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

# Union des deux datasets
df_joined = df_coin_std.unionByName(df_expat_std, allowMissingColumns=True)

print("Preview avant nettoyage supplémentaire:")
df_joined.show(20, truncate=False)

# ========== NETTOYAGES SUPPLÉMENTAIRES ==========

# 1. Supprimer les valeurs aberrantes dans 'type'
values_to_remove = ['Unknown', 'Immobilier', 'unknown', 'immobilier']
df_cleaned = df_joined.filter(~lower(col("type")).isin([v.lower() for v in values_to_remove]))

# 2. Renommer les valeurs dans 'type'
df_cleaned = df_cleaned.withColumn(
    "type",
    when(lower(col("type")) == "appartement", "appartements")
    .when(lower(col("type")) == "villa", "villas")
    .otherwise(col("type"))
)

# 3. Remplacer les superficies < 20 par la médiane
# Calculer la médiane avec approxQuantile
median_superficie = df_cleaned.approxQuantile("superficie", [0.5], 0.01)[0]
print(f"Médiane de superficie: {median_superficie}")

df_cleaned = df_cleaned.withColumn(
    "superficie",
    when(col("superficie") < 20, median_superficie)
    .otherwise(col("superficie"))
)

print(" Preview après nettoyage supplémentaire:")
df_cleaned.show(20, truncate=False)

# Statistiques
print(f"Nombre total de lignes après nettoyage: {df_cleaned.count()}")
print(f"Répartition par type:")
df_cleaned.groupBy("type").count().show()

# Sauvegarder les résultats
output_dir = "/tmp/joined_cleaned_data"
output_single = "/tmp/joined_cleaned_data_single"

df_cleaned.write.mode("overwrite").csv(output_dir, header=True)
df_cleaned.coalesce(1).write.mode("overwrite").option("header", True).csv(output_single)

print(f" Joined dataset saved to: {output_dir}")
print(f" Single CSV for joined dataset saved to: {output_single}")

spark.stop()