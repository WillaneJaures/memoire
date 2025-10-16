from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, split, trim, element_at, avg, rand, isnull, sum as spark_sum, count, mean
)
import os
import sys

# ============================
# 1️⃣  INITIALISATION SPARK
# ============================
spark = SparkSession.builder \
    .appName("ExpatDakar Transformation") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-9.3.0.jar") \
    .getOrCreate()

# ============================
# 2️⃣  LECTURE DU CSV EXPAT-DAKAR
# ============================
input_csv = "/tmp/expatdakar.csv"

if not os.path.exists(input_csv):
    print(f"❌ Error: File {input_csv} does not exist!")
    print(f"Current directory: {os.getcwd()}")
    print(f"Files in /tmp: {os.listdir('/tmp') if os.path.exists('/tmp') else 'Directory /tmp does not exist'}")
    spark.stop()
    sys.exit(1)

print(f"✅ File {input_csv} found, size: {os.path.getsize(input_csv)} bytes")

df = spark.read.csv(input_csv, header=True, sep=",", inferSchema=True)
df = df.toDF(*[col.lower() for col in df.columns])

print("Aperçu des données brutes ExpatDakar :")
df.show(10, truncate=False)

# ============================
# 3️⃣  SUPPRESSION DES DOUBLONS
# ============================
df1 = df.dropDuplicates()

# ============================
# 4️⃣  NETTOYAGE DES COLONNES m² DANS CHAMBRES/SDB
# ============================
df2 = df1.withColumn(
    "superficie",
    when(col("nombre_chambres").like("%m²%"), regexp_replace(col("nombre_chambres"), "m²", ""))
    .otherwise(col("superficie"))
).withColumn(
    "nombre_chambres",
    when(col("nombre_chambres").like("%m²%"), lit(None))
    .otherwise(col("nombre_chambres"))
)

df3 = df2.withColumn(
    "superficie",
    when(col("nombre_sdb").like("%m²%"), regexp_replace(col("nombre_sdb"), "m²", ""))
    .otherwise(col("superficie"))
).withColumn(
    "nombre_sdb",
    when(col("nombre_sdb").like("%m²%"), lit(None))
    .otherwise(col("nombre_sdb"))
)

# Nettoyage du prix: retirer non numériques
df4 = df3.withColumn("Price", regexp_replace(col("Price"), "[^0-9]", ""))

# ============================
# 5️⃣  REMPLISSAGE DES VALEURS MANQUANTES PAR MOYENNE
# ============================
def fillna_mean(df_in, include=set()):
    means = df_in.agg(*(mean(x).alias(x) for x in df_in.columns if x in include))
    return df_in.fillna(means.first().asDict())

df5 = fillna_mean(df4, {"Price", "nombre_chambres", "nombre_sdb", "superficie"})

# ============================
# 6️⃣  DERIVATION CATEGORY ET TYPE
# ============================
df6 = df5.withColumn(
    "category",
    when(col("Price").cast("int") >= 5000000, "vente").otherwise("location")
)

df7 = df6.withColumn(
    "type",
    when(col("nombre_chambres").cast("int") >= 4, "villa").otherwise("appartement")
)

# ============================
# 7️⃣  CAST FINAL ET NORMALISATION
# ============================
df8 = df7.withColumn("Price", col("Price").cast("int")) \
    .withColumn("nombre_chambres", col("nombre_chambres").cast("int")) \
    .withColumn("nombre_sdb", col("nombre_sdb").cast("int")) \
    .withColumn("superficie", col("superficie").cast("int"))

# Colonnes en minuscules si nécessaire (déjà en minuscules pour ce dataset)
df_clean = df8

print("✅ Aperçu des données transformées ExpatDakar:")
df_clean.show(20, truncate=False)

# ============================
# 8️⃣  SAUVEGARDE
# ============================
output_dir = "/tmp/expatdakar_cleaned_data"
output_single_csv = "/tmp/expatdakar_cleaned_data_single"

df_clean.write.mode("overwrite").csv(output_dir, header=True)
print(f"✅ Données ExpatDakar sauvegardées dans : {output_dir}")

df_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(output_single_csv)
print(f"✅ Fichier CSV unique ExpatDakar créé dans : {output_single_csv}")

spark.stop()

