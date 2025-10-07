from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, split, trim, element_at, avg, rand, isnull
)
import os
import sys

# ============================
# 1️⃣  INITIALISATION SPARK
# ============================
spark = SparkSession.builder \
    .appName("Data Transformation") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-9.3.0.jar") \
    .getOrCreate()

# ============================
# 2️⃣  LECTURE DU CSV BRUT
# ============================
input_csv = "/tmp/coinmarket.csv"

if not os.path.exists(input_csv):
    print(f"❌ Error: File {input_csv} does not exist!")
    print(f"Current directory: {os.getcwd()}")
    print(f"Files in /tmp: {os.listdir('/tmp') if os.path.exists('/tmp') else 'Directory /tmp does not exist'}")
    spark.stop()
    sys.exit(1)

print(f"✅ File {input_csv} found, size: {os.path.getsize(input_csv)} bytes")

df = spark.read.csv(input_csv, header=True, inferSchema=True)

print("Aperçu des données brutes :")
df.show(5, truncate=False)

# ============================
# 3️⃣  SUPPRESSION DES DOUBLONS
# ============================
df1 = df.dropDuplicates()
print(f"Nombre de lignes avant : {df.count()}, après suppression des doublons : {df1.count()}")

# ============================
# 4️⃣  NETTOYAGE DES COLONNES
# ============================
df2 = df1.withColumn(
    "Superficie",
    when(col("Nombre_de_piece").rlike("m2"), trim(col("Nombre_de_piece")))
    .otherwise(col("Superficie"))
).withColumn(
    "Nombre_de_piece",
    when(col("Nombre_de_piece").rlike("m2"), None)
    .otherwise(trim(col("Nombre_de_piece")))
)

df3 = df2.withColumn(
    "Superficie",
    when(col("Nombre_de_salle_bain").rlike("m2"), trim(col("Nombre_de_salle_bain")))
    .otherwise(col("Superficie"))
).withColumn(
    "Nombre_de_salle_bain",
    when(col("Nombre_de_salle_bain").rlike("m2"), None)
    .otherwise(trim(col("Nombre_de_salle_bain")))
)

df4 = df3.withColumn(
    "Superficie",
    trim(regexp_replace(col("Superficie"), "m2", ""))
)

# ============================
# 5️⃣  GESTION DU PRIX
# ============================
# Nettoyage : suppression des caractères non numériques
df4 = df4.withColumn("Price", regexp_replace(col("Price"), "[^0-9]", ""))

df5 = df4.withColumn(
    "prix_num",
    when(col("Price") != "", col("Price").cast("int")).otherwise(None)
)

# Calcul de la moyenne pour remplacer les valeurs "Prixsurdemande"
mean_price = df5.select(avg(col("prix_num"))).first()[0]
mean_price = int(mean_price) if mean_price else 0

df6 = df5.withColumn(
    "price",
    when(col("Price") == "", lit(mean_price)).otherwise(col("prix_num"))
)

# ============================
# 6️⃣  EXTRACTION DE CATEGORY
# ============================
df7 = df6.withColumn(
    "category",
    element_at(split(col("Description"), " "), 1)
)

df8 = df7.drop("posted_at", "prix_num", "Description")

# ============================
# 7️⃣  CORRECTIONS DE LOCATION / TYPE
# ============================
df9 = df8.withColumn(
    "Location_temp",
    when(col("Location") == "Appartements", col("Type")).otherwise(col("Location"))
).withColumn(
    "Type",
    when(col("Location") == "Appartements", col("Location")).otherwise(col("Type"))
).drop("Location").withColumnRenamed("Location_temp", "Location")

df10 = df9.withColumn(
    "Location_temp",
    when(col("Location") == "Villas", col("Type")).otherwise(col("Location"))
).drop("Location").withColumnRenamed("Location_temp", "Location")

df11 = df10.withColumn(
    "Location_temp",
    when(col("Location") == "Terrains", col("Type")).otherwise(col("Location"))
).drop("Location").withColumnRenamed("Location_temp", "Location")

# ============================
# 8️⃣  NORMALISATION DE CATEGORY
# ============================
df12 = df11.withColumn("category", regexp_replace(col("category"), "Terrains?", "Terrain"))
df13 = df12.withColumn("category", regexp_replace(col("category"), "(?i)locations?", "Location"))
df14 = df13.withColumn("category", regexp_replace(col("category"), "Locaton|Location/", "Location"))
df15 = df14.withColumn("category", regexp_replace(col("category"), "Terrain|Six|Parcelle|100", "Vente"))

df16 = df15.withColumn(
    "category",
    when((col("category") == "Location") | (col("category") == "Vente"), col("category"))
    .otherwise("Vente")
)

# ============================
# 9️⃣  AJUSTEMENTS TYPE / LOCATION / CITY
# ============================
df17 = df16.withColumn(
    "Type",
    when(col("Type").rlike("Sénégal"), "Appartements").otherwise(col("Type"))
)

df18 = df17.withColumn(
    "Area",
    element_at(split(col("Location"), ","), 1)
)

df19 = df18.withColumn(
    "Area",
    when(col("Area").rlike("(?i)(Immeubles|Bureaux & Commerces|Fermes & Vergers|Maisons de vacances|Terrains agricoles|Appartements meublés)"), "Dakar")
    .otherwise(col("Area"))
)

df20 = df19.withColumn(
    "City",
    when(col("Location").rlike("(?i)Dakar"), "Dakar")
    .when(col("Location").rlike("(?i)Thies"), "Thies")
    .when(col("Location").rlike("(?i)Saint-Louis"), "Saint-Louis")
    .when(col("Location").rlike("(?i)Kaolack"), "Kaolack")
    .when(col("Location").rlike("(?i)Ziguinchor"), "Ziguinchor")
    .when(col("Location").rlike("(?i)Louga"), "Louga")
    .when(col("Location").rlike("(?i)Saly"), "Saly")
    .when(col("Location").rlike("(?i)Keur Massar"), "Keur Massar")
    .when(col("Location").rlike("(?i)Diamniadio"), "Diamniadio")
    .when(col("Location").rlike("(?i)Mbour"), "Mbour")
    .when(col("Location").rlike("(?i)Lac rose"), "Lac rose")
    .when(col("Location").rlike("(?i)Rufisque"), "Rufisque")
    .when(col("Location").rlike("(?i)Ndiass"), "Ndiass")
    .when(col("Location").rlike("(?i)Kolda"), "Kolda")
    .when(col("Location").rlike("(?i)Ngaparou"), "Ngaparou")
    .when(col("Location").rlike("(?i)Niaga"), "Niaga")
    .when(col("Location").rlike("(?i)Toubab Dialao"), "Toubab Dialao")
    .when(col("Location").rlike("(?i)Mboro"), "Mboro")
    .otherwise("Null")
)

# Remplissage aléatoire des zones de Dakar
choices = ["Almadies", "Yoff", "Ngor", "Point E", "Mamelles"]
df21 = df20.withColumn(
    "Area",
    when(col("Area") == "Dakar",
         when(rand() < 0.2, choices[0])
         .when(rand() < 0.4, choices[1])
         .when(rand() < 0.6, choices[2])
         .when(rand() < 0.8, choices[3])
         .otherwise(choices[4])
    ).otherwise(col("Area"))
)

df22 = df21.withColumn(
    "City",
    when(col("City") == "Null", "Dakar").otherwise(col("City"))
).drop("Location")

# ============================
# 🔟  REMPLISSAGE VALEURS MANQUANTES
# ============================
df23 = df22.withColumn(
    "Nombre_de_piece",
    when(isnull(col("Nombre_de_piece")),
         when(rand() < 0.2, "4")
         .when(rand() < 0.4, "3")
         .when(rand() < 0.6, "2")
         .when(rand() < 0.8, "5")
         .otherwise("1")
    ).otherwise(col("Nombre_de_piece"))
)

df24 = df23.withColumn(
    "Nombre_de_salle_bain",
    when(isnull(col("Nombre_de_salle_bain")),
         when(rand() < 0.2, "2")
         .when(rand() < 0.4, "5")
         .when(rand() < 0.6, "3")
         .when(rand() < 0.8, "4")
         .otherwise("1")
    ).otherwise(col("Nombre_de_salle_bain"))
)

df25 = df24.withColumn(
    "Superficie",
    when(isnull(col("Superficie")),
         when(rand() < 0.2, "100")
         .when(rand() < 0.4, "200")
         .when(rand() < 0.6, "300")
         .when(rand() < 0.8, "400")
         .otherwise("500")
    ).otherwise(col("Superficie"))
)

# ============================
# 1️⃣1️⃣  CONVERSION FINALE
# ============================
df_clean = df25.withColumn("Nombre_de_piece", col("Nombre_de_piece").cast("int")) \
    .withColumn("Nombre_de_salle_bain", col("Nombre_de_salle_bain").cast("int")) \
    .withColumn("Superficie", col("Superficie").cast("int")) \
    .withColumn("price", col("price").cast("int"))

print("✅ Aperçu des données transformées :")
df_clean.show(20, truncate=False)

# ============================
# 1️⃣2️⃣  SAUVEGARDE
# ============================
output_dir = "/tmp/cleaned_data"
output_single_csv = "/tmp/cleaned_data_single"

df_clean.write.mode("overwrite").csv(output_dir, header=True)
print(f"✅ Données sauvegardées dans le dossier : {output_dir}")

# ✅ Version avec un seul CSV (pour Streamlit et SQLite)
df_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(output_single_csv)
print(f"✅ Fichier CSV unique créé dans : {output_single_csv}")

spark.stop()
