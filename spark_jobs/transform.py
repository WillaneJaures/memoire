from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace,split,trim, element_at, when, array_contains,try_element_at,size,expr,lower,count,mean,desc,asc,avg,round, regexp_extract, rand, isnull
import os
import sys
import logging
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

spark = SparkSession.builder \
    .appName("Data Transformation") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-9.3.0.jar") \
    .getOrCreate()


#read the csv file
input_csv = "/tmp/coinmarket.csv"

#verfife que le fichier existe
if not os.path.exists(input_csv):
    print(f"❌ Error: File {input_csv} does not exist!")
    print(f"Current directory: {os.getcwd()}")
    print(f"Files in /tmp: {os.listdir('/tmp') if os.path.exists('/tmp') else 'Directory /tmp does not exist'}")
    spark.stop()
    sys.exit(1)

    
print(f"✅ File {input_csv} found, size: {os.path.getsize(input_csv)} bytes")

df = spark.read.csv(input_csv, header=True, inferSchema=True)

# Affiche les 5 premières lignes
print("Aperçu des données brutes :")
df.show(5)


# supprimons les doublons
df1 = df.dropDuplicates()
print(f"nombres de lignes avant la suppression des doublons: {df.count()}")
print(f"nombres de lignes apres la suppression des doublons: {df1.count()}")

#count missing values
#missing_values = df1.select([count(when(col(c).isNull(), c)).alias(c) for c in df1.columns]).show()

#si the column nbr de piece contains m2 place it to superficie and set the column nbr de piece to None
df2 = df1.withColumn(
    "Superficie",
    when(col("Nombre_de_piece").rlike("m2"), trim(col("Nombre_de_piece")))
    .otherwise(col("Superficie"))
).withColumn(
    "Nombre_de_piece",
    when(col("Nombre_de_piece").rlike("m2"), None)
    .otherwise(trim(col("Nombre_de_piece")))
)

#if the column nbr de salle de bain contains m2 place it to superficie and set the column nbr de salle de bain to None

df3 = df2.withColumn(
    "Superficie",
    when(col("Nombre_de_salle_bain").rlike("m2"), trim(col("Nombre_de_salle_bain")))
    .otherwise(col("Superficie"))
).withColumn(
    "Nombre_de_salle_bain",
    when(col("Nombre_de_salle_bain").rlike("m2"), None)
    .otherwise(trim(col("Nombre_de_salle_bain")))
)

#remote m2 from superficie column

df4 = df3.withColumn(
    "Superficie",
    trim(regexp_replace(col("Superficie"), "m2", ""))
    )

#creation new column price_num

df5 = df4.withColumn(
    "prix_num",
    when(col("Price") != "Prixsurdemande", col("Price").cast("int")).otherwise(None))

mean_price = df5.select(avg(col("prix_num"))).first()[0]
mean_price = int(mean_price)

df6 = df5.withColumn(
    "price",
    when(col("Price") == "Prixsurdemande", lit(mean_price)).otherwise(col("Price")))

#split column description

df7 = df6.withColumn(
    "category",
    element_at(split(col("Description"), " "), 1))

df8 = df7.drop("posted_at", "prix_num", "Description")

df9 = df8.withColumn(
    "Location_temp",
    when(col("Location") == "Appartements", col("Type")).otherwise(col("Location"))
).withColumn(
    "Type",
    when(col("Location") == "Appartements", col("Location")).otherwise(col("Type"))
).withColumnRenamed("Location", "Location_old") \
 .withColumnRenamed("Location_temp", "Location") \
 .drop("Location_old")


df10 = df9.withColumn(
    "Location_temp",
    when(col("Location") == "Villas", col("Type")).otherwise(col("Location"))
).withColumnRenamed("Location", "Location_old") \
 .withColumnRenamed("Location_temp", "Location").drop("Location_old")


df11 = df10.withColumn(
    "Location_temp",
    when(col("Location") == "Terrains", col("Type")).otherwise(col("Location"))
).withColumnRenamed("Location", "Location_old") \
 .withColumnRenamed("Location_temp", "Location").drop("Location_old"
)

df12 = df11.withColumn(
    "category",
    regexp_replace(col("category"), "Terrains", "Terrain"))

df13 = df12.withColumn(
    "category",
    regexp_replace(col("category"), "terrain", "Terrain"))


df14 = df13.withColumn(
    "category",
    regexp_replace(col("category"), "(?i)locations?", "Location")
)

df15 = df14.withColumn(
    "category",
    regexp_replace(regexp_replace(col("category"), "Locaton", "Location"), "Location/", "Location")
)

#df15.show(5)
#df15.groupBy("price","category", "Type", "Location").count().orderBy(desc("count")).show(100)
#df15.select("price","category", "Type", "Location").distinct().show(100)

df16 = df15.withColumn(
    "category",
    regexp_replace(regexp_replace(col("category"), "Terrain", "Vente"), "Six", "Vente")
)

df17 = df16.withColumn(
    "category",
    regexp_replace(regexp_replace(col("category"), "Parcelle", "Vente"), "100", "Vente")
)


df18 = df17.withColumn(
    "category",
    when(
        (col("category") == "Location") | (col("category") == "Vente"),
        col("category")
    ).otherwise("Vente")
)

df19 = df18.withColumn(
    "Type",
    when( col("Type").rlike("Sénégal"), "Appartements")
    .otherwise(col("Type"))
        
)

df20 = df19.withColumn(
    "Area",
    element_at(split(col("Location"), ","), 1)
)

df21 = df20.withColumn(
    "Area",
    when(col("Area").rlike("Immeubles"), "Dakar").
    when(col("Area").rlike("Bureaux & Commerces"), "Dakar").
    when(col("Area").rlike("Fermes & Vergers"), "Dakar").
    when(col("Area").rlike("Maisons de vacances"), "Dakar").
    when(col("Area").rlike("Terrains agricoles"), "Dakar").
    when(col("Area").rlike("Appartements meublés"), "Dakar").
    otherwise(col("Area"))
)

#df21.show(10, truncate=False)
df22 = df21.withColumn(
    "City",
    when(col("Location").rlike("(?i)Dakar?"), "Dakar")
    .when(col("Location").rlike("(?i)Thies?"), "Thies")
    .when(col("Location").rlike("(?i)Saint-Louis?"), "Saint-Louis")
    .when(col("Location").rlike("(?i)Kaolack?"), "Kaolack")
    .when(col("Location").rlike("(?i)Ziguinchor?"), "Ziguinchor")
    .when(col("Location").rlike("(?i)Louga?"), "Louga")
    .when(col("Location").rlike("(?i)Saly?"), "Saly")
    .when(col("Location").rlike("(?i)Keur Massar?"), "Keur Massar")
    .when(col("Location").rlike("(?i)Diamniadio?"), "Diamniadio")
    .when(col("Location").rlike("(?i)Mbour?"), "Mbour")
    .when(col("Location").rlike("(?i)Lac rose?"), "Lac rose")
    .when(col("Location").rlike("(?i)Rufisque?"), "Rufisque")
    .when(col("Location").rlike("(?i)Ndiass?"), "Ndiass")
    .when(col("Location").rlike("(?i)Kolda?"), "Kolda")
    .when(col("Location").rlike("(?i)Ngaparou?"), "Ngaparou")
    .when(col("Location").rlike("(?i)Niaga?"), "Niaga")
    .when(col("Location").rlike("(?i)Toubab Dialao?"), "Toubab Dialao")
    .when(col("Location").rlike("(?i)Mboro?"), "Mboro")
    .otherwise("Null")
)

choices = ["Almadies", "Yoff", "Ngor", "Point E", "Mamelles"]
df23 = df22.withColumn(
    "Area",
    when(col("Area") == "Dakar", 
         when(rand() < 0.2, choices[0])
        .when(rand() < 0.4, choices[1])
        .when(rand() < 0.6, choices[2])
        .when(rand() < 0.8, choices[3])
        .otherwise(choices[4])
    ).otherwise(col("Area"))
)

df24 = df23.withColumn(
    "City",
    when(col("City").rlike("Null"), "Dakar")
    .otherwise(col("City"))
)

df25 = df24.drop("Location")

df26 = df25.withColumn(
    "Nombre_de_piece",
    when(isnull(col("Nombre_de_piece")), 
         when(rand() < 0.2, "4")
        .when(rand() < 0.4, "3")
        .when(rand() < 0.6, "2")
        .when(rand() < 0.8, "5")
        .otherwise("1")
    ).otherwise(col("Nombre_de_piece"))
)

df27 = df26.withColumn(
    "Nombre_de_salle_bain",
    when(isnull(col("Nombre_de_salle_bain")), 
         when(rand() < 0.2, "2")
        .when(rand() < 0.4, "5")
        .when(rand() < 0.6, "3")
        .when(rand() < 0.8, "4")
        .otherwise("1")
    ).otherwise(col("Nombre_de_salle_bain"))
)

df28 = df27.withColumn(
    "Superficie",
    when(isnull(col("Superficie")), 
         when(rand() < 0.2, "100")
        .when(rand() < 0.4, "200")
        .when(rand() < 0.6, "300")
        .when(rand() < 0.8, "400")
        .otherwise("500")
    ).otherwise(col("Superficie"))
)

#df28.groupBy("Superficie").count().orderBy(desc("count")).show(100)

df_clean = df28.withColumn("Nombre_de_piece", col("Nombre_de_piece").cast("int")) \
    .withColumn("Nombre_de_salle_bain", col("Nombre_de_salle_bain").cast("int")) \
    .withColumn("Superficie", col("Superficie").cast("int")) \


print("Aperçu des données transformées :")
df_clean.show(20)
output_csv = "/tmp/cleaned_data.csv"
df_clean.write.mode("overwrite").csv(output_csv, header=True)
print(f"✅ Data saved to {output_csv}")

        
spark.stop()






