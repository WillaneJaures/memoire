from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import datetime, timedelta
import logging
import time


# ----------------- Default Args -----------------
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# ----------------- DAG Definition -----------------
with DAG(
    dag_id='coindakaretl',
    start_date=datetime(2025, 7, 22),
    schedule='@hourly',
    catchup=False,
    default_args=default_args,
    description="ETL DAG to scrape data from CoinAfrique"
) as dag:

    # ----------------- Scraping Task -----------------
    @task
    def scrape_coinmarket():
        # Lazy imports to speed up DAG parse time
        import pandas as pd
        from bs4 import BeautifulSoup as bs
        from requests import get
        import time

        df = pd.DataFrame()
        for i in range(1, 5):  
            url = f'https://sn.coinafrique.com/categorie/immobilier?page={i}'
            logging.info(f"🔎 Scraping page {i}: {url}")
            res = get(url)
            soup = bs(res.text, 'html.parser')
            info_all = soup.find_all('div', class_='col s6 m4 l3')
            links = ['https://sn.coinafrique.com' + a.find('a')['href'] for a in info_all]

            data = []
            for x in links:
                try:
                    res = get(x)
                    soup = bs(res.text, 'html.parser')

                    price_inf = soup.find('p', class_='price')
                    price = price_inf.text.replace(" ", "").replace("CFA", "") if price_inf else ''

                    description_tag = soup.find('h1', class_='title title-ad hide-on-large-and-down')
                    description = description_tag.text if description_tag else ''

                    info2 = soup.find_all("span", class_="valign-wrapper")
                    time_p = info2[0].text if len(info2) > 0 else ''
                    location = info2[1].text if len(info2) > 1 else ''
                    type_ = info2[2].text if len(info2) > 2 else ''

                    info3 = soup.find_all('span', class_="qt")
                    nbr_p = info3[0].text if len(info3) > 0 else ''
                    nbr_sb = info3[1].text if len(info3) > 1 else ''
                    sup = info3[2].text if len(info3) > 2 else ''

                    obj = {
                        'Price': price,
                        'Description': description,
                        'Location': location,
                        'posted_at': time_p,
                        'Type': type_,
                        'Nombre_de_piece': nbr_p,
                        'Nombre_de_salle_bain': nbr_sb,
                        'Superficie': sup
                    }

                    data.append(obj)
                    time.sleep(1)

                except Exception as e:
                    logging.error(f"❌ Error parsing page: {x}, {e}")
                    continue

            logging.info(f"✅ Scraped page {i}, total entries so far: {len(data)}")
            df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)

        if not df.empty:
            df.to_csv('/tmp/coinmarket.csv', index=False)
            logging.info("✅ Data scraped and saved to /tmp/coinmarket.csv")
        else:
            logging.warning("⚠️ No data scraped, DataFrame is empty.")


    @task
    def scrape_expatdakar():
        """Scrape les appartements sur expat-dakar et enregistre dans /tmp/expatdakar.csv"""
        import scrapy
        from scrapy.crawler import CrawlerProcess
        import re, pandas as pd, logging, os

        results = []  # ✅ conteneur global pour stocker les résultats

        class ExpatDakarSpider(scrapy.Spider):
            name = "expat_dakar"
            allowed_domains = ["expat-dakar.com"]
            start_urls = [f"https://www.expat-dakar.com/appartements-a-louer?page={i}" for i in range(1, 4)]
            handle_httpstatus_list = [403, 429]
            custom_settings = {
                'LOG_ENABLED': False,
                'ROBOTSTXT_OBEY': False,
                'DOWNLOAD_DELAY': 0.75,
                'CONCURRENT_REQUESTS': 4,
                'RETRY_ENABLED': True,
                'RETRY_TIMES': 3,
                'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 504],
                'DEFAULT_REQUEST_HEADERS': {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                },
            }

            USER_AGENTS = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            ]

            REFERERS = [
                'https://www.google.com/',
                'https://www.bing.com/',
                'https://www.expat-dakar.com/',
            ]

            def start_requests(self):
                import random
                for url in self.start_urls:
                    headers = {
                        'User-Agent': random.choice(self.USER_AGENTS),
                        'Referer': random.choice(self.REFERERS),
                    }
                    yield scrapy.Request(url, headers=headers, dont_filter=True)

            def parse(self, response):
                if response.status in (403, 429):
                    self.logger.info(f"Ignored response {response.status} for {response.url}")
                    return
                links = response.css("a.listing-card__inner::attr(href)").getall()
                if not links:
                    links = response.css("a[href*='/appartement']::attr(href)").getall()

                for link in links:
                    yield response.follow(link, callback=self.parse_annonce)

                current_page = response.url.split("page=")[-1] if "page=" in response.url else "1"
                print(f"✅ Page {current_page} terminée ({response.url})")

            def parse_annonce(self, response):
                clean = lambda t: re.sub(r'\s+', ' ', t.strip()) if t else ''
                infos = [clean(i) for i in response.css("dd.listing-item__properties__description::text").getall()]
                superficie = infos[2].replace(" ", "").replace("m²", "").strip() if len(infos) > 2 else ''

                Price = clean(response.css("span.listing-card__price__value::text").get())
                quartier = clean(response.css("span.listing-item__address-location::text").get())
                region = clean(response.css("span.listing-item__address-region::text").get())

                # ✅ on ajoute dans la liste globale, pas dans self
                results.append({
                    "Price": Price.replace(" ", "").replace('FCfa', ''),
                    "quartier": quartier,
                    "region": region,
                    "nombre_chambres": infos[0] if len(infos) > 0 else '',
                    "nombre_sdb": infos[1] if len(infos) > 1 else '',
                    "superficie": superficie,
                    #"url": response.url
                })

        logging.info("Lancement du scraping Expat-Dakar...")
        process = CrawlerProcess(settings={"USER_AGENT": "Mozilla/5.0", "FEED_EXPORT_ENCODING": "utf-8"})
        process.crawl(ExpatDakarSpider)
        process.start()

        # ✅ Après la fin du crawl, on accède directement à la liste globale
        logging.info(f"🔍 Total results collected: {len(results)}")

        df = pd.DataFrame(results)
        output_path = "/tmp/expatdakar.csv"
        if not df.empty:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logging.info(f"✅ {len(df)} annonces sauvegardées dans {output_path}")
        else:
            logging.warning("⚠️ Aucun résultat trouvé sur Expat-Dakar.")

        return output_path

    
    #------------------minio upload task ------------------
    # upload scraped data to minio
    @task
    def upload_to_minio():
        # Lazy imports to speed up DAG parse time
        import boto3
        from botocore.exceptions import NoCredentialsError, PartialCredentialsError

        # Essayer différentes méthodes de connexion
        endpoints_to_try = [
            'http://memoire_5455b9-minio-1:9000',
            'http://172.18.0.3:9000'
                   
        ]
        
        s3_client = None
        for endpoint in endpoints_to_try:
            try:
                logging.info(f"🔗 Tentative de connexion à: {endpoint}")
                s3_client = boto3.client(
                    's3',
                    endpoint_url=endpoint,
                    aws_access_key_id='minio',
                    aws_secret_access_key='minio123',
                    region_name='us-east-1'
                )
                # Test rapide de connexion
                s3_client.list_buckets()
                logging.info(f"✅ Connexion réussie avec: {endpoint}")
                break
            except Exception as e:
                logging.warning(f"❌ Échec avec {endpoint}: {e}")
                continue
        
        if s3_client is None:
            raise Exception("❌ Impossible de se connecter à MinIO avec aucune des méthodes")
        
        # Afficher les buckets disponibles
        try:
            response = s3_client.list_buckets()
            logging.info(f"📦 Buckets disponibles: {len(response['Buckets'])}")
            for bucket in response['Buckets']:
                logging.info(f"   📦 {bucket['Name']}")
        except Exception as e:
            logging.error(f"❌ Erreur lors de la liste des buckets: {e}")
            raise

        bucket_name = 'coinmarketbucket'
        file_path = '/tmp/coinmarket.csv'
        object_name = 'coinmarket.csv'

        try:
            # Check if file exists
           
            if not os.path.exists(file_path):
                logging.error(f"❌ File not found: {file_path}")
                return
            
            # Check if bucket exists, create if it doesn't
            try:
                s3_client.head_bucket(Bucket=bucket_name)
                logging.info(f"✅ Bucket '{bucket_name}' exists")
            except Exception as e:
                logging.info(f"📦 Bucket '{bucket_name}' doesn't exist, creating it...")
                s3_client.create_bucket(Bucket=bucket_name)
                logging.info(f"✅ Bucket '{bucket_name}' created successfully")
            
            # Upload the file
            s3_client.upload_file(file_path, bucket_name, object_name)
            logging.info(f"✅ File uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
            
        except FileNotFoundError:
            logging.error("❌ The file was not found")
        except NoCredentialsError:
            logging.error("❌ Credentials not available")
        except PartialCredentialsError:
            logging.error("❌ Incomplete credentials provided")
        except Exception as e:
            logging.error(f"❌ Error uploading file: {e}")
            logging.error(f"❌ Error details: {type(e).__name__}: {str(e)}")
        
        

    #----------------------Verify csv available
    @task
    def verify_csv_exists():
        coinmarket_path = '/tmp/coinmarket.csv'
        expat_path = '/tmp/expatdakar.csv'
        ok = True
        if os.path.exists(coinmarket_path):
            size = os.path.getsize(coinmarket_path)
            logging.info(f"✅ coinmarket.csv exists, size: {size} bytes")
        else:
            logging.error(f"❌ CSV file {coinmarket_path} does not exist!")
            ok = False
        if os.path.exists(expat_path):
            size = os.path.getsize(expat_path)
            logging.info(f"✅ expatdakar.csv exists, size: {size} bytes")
        else:
            logging.error(f"❌ CSV file {expat_path} does not exist!")
            ok = False
        if not ok:
            try:
                files_in_tmp = os.listdir('/tmp')
                logging.info(f"Files in /tmp: {files_in_tmp}")
            except Exception as e:
                logging.error(f"Error listing /tmp: {e}")
        return ok
        
   

    # ----------------- Définition des tâches et dépendances -----------------
 
    
    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/usr/local/airflow/spark_jobs/transform.py",
        conn_id="spark_default",
        application_args=["/tmp/coinmarket.csv"],
        dag=dag

    )

    # Spark job for ExpatDakar transformation
    spark_job_expat = SparkSubmitOperator(
        task_id="spark_job_expat",
        application="/usr/local/airflow/spark_jobs/transform1.py",
        conn_id="spark_default",
        application_args=["/tmp/expatdakar.csv"],
        dag=dag
    )

     #---------------------------upload to minio
    @task
    def upload_tranform_minio():
        import boto3, sys

        minio_urls = [
            'memoire_5455b9-minio-1:9000',
            'http://172.18.0.3:9000'
            
        ]
        
        s3_client = None
        for endpoint in minio_urls:
            try:
                logging.info(f"🔗 Tentative de connexion à: {endpoint}")
                s3_client = boto3.client(
                    's3',
                    endpoint_url=endpoint,
                    aws_access_key_id='minio',
                    aws_secret_access_key='minio123',
                    region_name='us-east-1'
                )
                s3_client.list_buckets()  # Test rapide
                logging.info(f"✅ Connexion réussie avec: {endpoint}")
                break
            except Exception as e:
                logging.warning(f"❌ Échec avec {endpoint}: {e}")
        
        if s3_client is None:
            raise Exception("❌ Impossible de se connecter à MinIO")

        bucket_name = 'coinmarkettransform'
        file_path = '/tmp/cleaned_data_single'
        object_name = 'cleaned_data.csv'

        if not os.path.exists(file_path):
             logging.error(f"❌ File not found: {file_path}")
             sys.exit(1)

         # Vérifier si c'est un fichier ou un répertoire
        if os.path.isdir(file_path):
             logging.warning(f"⚠️ {file_path} is a directory, looking for CSV files inside...")
             csv_files = [f for f in os.listdir(file_path) if f.endswith('.csv')]
             if csv_files:
                 # Prendre le premier fichier CSV trouvé
                 actual_file = os.path.join(file_path, csv_files[0])
                 logging.info(f"📁 Found CSV file: {actual_file}")
                 file_path = actual_file
             else:
                 logging.error(f"❌ No CSV files found in directory {file_path}")
                 sys.exit(1)

        try:
             # Vérifier/Créer bucket
             try:
                 s3_client.head_bucket(Bucket=bucket_name)
                 logging.info(f"✅ Bucket '{bucket_name}' existe")
             except Exception:
                 logging.info(f"📦 Création du bucket '{bucket_name}'...")
                 s3_client.create_bucket(Bucket=bucket_name)

             # Upload fichier
             s3_client.upload_file(file_path, bucket_name, object_name)
             logging.info(f"✅ Fichier {file_path} uploadé dans MinIO -> {bucket_name}/{object_name}")

        except Exception as e:
             logging.error(f"❌ Erreur upload MinIO: {e}")

    @task
    def upload_expat_raw_to_minio():
        import boto3
        
        endpoints_to_try = [
            'http://memoire_5455b9-minio-1:9000',
            'http://172.18.0.3:9000'
        ]
        s3_client = None
        for endpoint in endpoints_to_try:
            try:
                logging.info(f"🔗 Tentative de connexion à: {endpoint}")
                s3_client = boto3.client(
                    's3',
                    endpoint_url=endpoint,
                    aws_access_key_id='minio',
                    aws_secret_access_key='minio123',
                    region_name='us-east-1'
                )
                s3_client.list_buckets()
                logging.info(f"✅ Connexion réussie avec: {endpoint}")
                break
            except Exception as e:
                logging.warning(f"❌ Échec avec {endpoint}: {e}")
                continue
        if s3_client is None:
            raise Exception("❌ Impossible de se connecter à MinIO")
        bucket_name = 'expatdakarbucket'
        file_path = '/tmp/expatdakar.csv'
        object_name = 'expatdakar.csv'
        if not os.path.exists(file_path):
            logging.error(f"❌ File not found: {file_path}")
            return
        try:
            try:
                s3_client.head_bucket(Bucket=bucket_name)
            except Exception:
                s3_client.create_bucket(Bucket=bucket_name)
            s3_client.upload_file(file_path, bucket_name, object_name)
            logging.info(f"✅ File uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
        except Exception as e:
            logging.error(f"❌ Error uploading file: {e}")

    @task
    def upload_expat_transform_minio():
        import boto3, sys
        
        minio_urls = [
            'memoire_5455b9-minio-1:9000',
            'http://172.18.0.3:9000'
        ]
        s3_client = None
        for endpoint in minio_urls:
            try:
                logging.info(f"🔗 Tentative de connexion à: {endpoint}")
                s3_client = boto3.client(
                    's3',
                    endpoint_url=endpoint,
                    aws_access_key_id='minio',
                    aws_secret_access_key='minio123',
                    region_name='us-east-1'
                )
                s3_client.list_buckets()
                logging.info(f"✅ Connexion réussie avec: {endpoint}")
                break
            except Exception as e:
                logging.warning(f"❌ Échec avec {endpoint}: {e}")
        if s3_client is None:
            raise Exception("❌ Impossible de se connecter à MinIO")
        bucket_name = 'expatdakatransform'
        file_path = '/tmp/expatdakar_cleaned_data_single'
        object_name = 'expatdakar_cleaned.csv'
        if not os.path.exists(file_path):
            logging.error(f"❌ File not found: {file_path}")
            sys.exit(1)
        if os.path.isdir(file_path):
            csv_files = [f for f in os.listdir(file_path) if f.endswith('.csv')]
            if csv_files:
                file_path = os.path.join(file_path, csv_files[0])
            else:
                logging.error(f"❌ No CSV files found in directory {file_path}")
                sys.exit(1)
        try:
            try:
                s3_client.head_bucket(Bucket=bucket_name)
            except Exception:
                s3_client.create_bucket(Bucket=bucket_name)
            s3_client.upload_file(file_path, bucket_name, object_name)
            logging.info(f"✅ Fichier {file_path} uploadé dans MinIO -> {bucket_name}/{object_name}")
        except Exception as e:
            logging.error(f"❌ Erreur upload MinIO: {e}")


    
    @task
    def upload_join_to_sqlite():
        """Upload joined data to SQLite database - creates 'realestate' table"""
        import pandas as pd
        import sqlite3
        import os
        
        # Chemin vers la base de données SQLite
        db_path = '/usr/local/airflow/immobilier.db'
        
        # Chemin vers les données jointes
        file_path = '/tmp/joined_cleaned_data_single'
        
        # Vérifier si c'est un fichier ou un répertoire
        if os.path.isdir(file_path):
            csv_files = [f for f in os.listdir(file_path) if f.endswith('.csv')]
            if csv_files:
                actual_file = os.path.join(file_path, csv_files[0])
                file_path = actual_file
            else:
                logging.error(f"❌ No CSV files found in directory {file_path}")
                return False
        
        try:
            # Lire le CSV
            df = pd.read_csv(file_path)
            logging.info(f"✅ Data read from {file_path}, shape: {df.shape}")
            
            # Connexion à SQLite
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Créer la table 'realestate' avec le bon schéma
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS realestate (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    price INTEGER,
                    type TEXT,
                    superficie INTEGER,
                    nombre_chambres INTEGER,
                    nombre_sdb INTEGER,
                    category TEXT,
                    area TEXT,
                    city TEXT,
                    source TEXT
                )
            """)
            
            # Supprimer les données existantes et insérer les nouvelles
            cursor.execute("DELETE FROM realestate")
            
            # Préparer les données pour l'insertion
            df_clean = df.copy()
            
            # Nettoyer et convertir les types de données
            if 'price' in df_clean.columns:
                df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce').fillna(0).astype(int)
            if 'superficie' in df_clean.columns:
                df_clean['superficie'] = pd.to_numeric(df_clean['superficie'], errors='coerce').fillna(0).astype(int)
            if 'nombre_chambres' in df_clean.columns:
                df_clean['nombre_chambres'] = pd.to_numeric(df_clean['nombre_chambres'], errors='coerce').fillna(0).astype(int)
            if 'nombre_sdb' in df_clean.columns:
                df_clean['nombre_sdb'] = pd.to_numeric(df_clean['nombre_sdb'], errors='coerce').fillna(0).astype(int)
            
            # Remplir les valeurs manquantes pour les colonnes texte
            text_columns = ['type', 'category', 'area', 'city', 'source']
            for col in text_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].fillna('Unknown')
                else:
                    df_clean[col] = 'Unknown'
            
            # Insérer les données
            df_clean.to_sql('realestate', conn, if_exists='append', index=False)
            
            # Vérifier les données insérées
            cursor.execute("SELECT COUNT(*) FROM realestate")
            count = cursor.fetchone()[0]
            logging.info(f"✅ {count} records inserted into SQLite table 'realestate'")
            
            # Afficher un aperçu des données
            cursor.execute("SELECT * FROM realestate LIMIT 5")
            rows = cursor.fetchall()
            logging.info(f"✅ Preview of SQLite data: {len(rows)} rows shown")
            
            # Afficher les colonnes de la table
            cursor.execute("PRAGMA table_info(realestate)")
            columns_info = cursor.fetchall()
            logging.info(f"✅ Table 'realestate' schema: {[col[1] for col in columns_info]}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logging.error(f"❌ Error uploading to SQLite: {e}")
            return False

    # ----------------- Orchestration (instantiate tasks and set dependencies) -----------------
    # Phase 1 : Scraping des deux sources en parallèle
    coinmarket_task = scrape_coinmarket()
    expatdakar_task = scrape_expatdakar()

    # Phase 2 : Upload des données brutes sur MinIO en parallèle
    upload_coinmarket_raw = upload_to_minio()
    upload_expat_raw = upload_expat_raw_to_minio()

    # Chaque source scrape puis upload ses données
    coinmarket_task >> upload_coinmarket_raw
    expatdakar_task >> upload_expat_raw

    # Phase 3 : Vérification que les fichiers CSV existent
    verify_csv = verify_csv_exists()
    [upload_coinmarket_raw, upload_expat_raw] >> verify_csv

    # Phase 4 : Transformation Spark en parallèle (operators already defined above)
    verify_csv >> [spark_job, spark_job_expat]

    # Phase 5 : Upload des données transformées sur MinIO en parallèle
    upload_coinmarket_transform = upload_tranform_minio()
    upload_expat_transform = upload_expat_transform_minio()

    # Chaque Spark job upload son résultat transformé
    spark_job >> upload_coinmarket_transform
    spark_job_expat >> upload_expat_transform

    # Phase 6 : Jointure des datasets transformés
    join_job = SparkSubmitOperator(
        task_id="spark_join_job",
        application="/usr/local/airflow/spark_jobs/join_datasets.py",
        conn_id="spark_default",
        dag=dag
    )

    # La jointure attend les deux uploads transformés
    [upload_coinmarket_transform, upload_expat_transform] >> join_job

    # Phase 7 : Upload de la jointure sur SQLite
    upload_join_sqlite = upload_join_to_sqlite()
    
    # L'upload SQLite attend la jointure
    join_job >> upload_join_sqlite
