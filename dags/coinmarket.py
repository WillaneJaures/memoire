from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import datetime, timedelta
import logging
import time
from elasticsearch import Elasticsearch


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
        for i in range(1, 5):  # Pages 1 à 4
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

    scraped = scrape_coinmarket()

 
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
            'http://172.18.0.2:9000'
                   
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
        csv_path = '/tmp/coinmarket.csv'
        if os.path.exists(csv_path):
            size = os.path.getsize(csv_path)
            logging.info(f"✅ CSV file exists, size: {size} bytes")
            return True
        else:
            logging.error(f"❌ CSV file {csv_path} does not exist!")
            try:
                files_in_tmp = os.listdir('/tmp')
                logging.info(f"Files in /tmp: {files_in_tmp}")
            except Exception as e:
                logging.error(f"Error listing /tmp: {e}")
            return False
        
   

    # ----------------- Définition des tâches et dépendances -----------------
 
    
    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/usr/local/airflow/spark_jobs/transform.py",
        conn_id="spark_default",
        application_args=["/tmp/coinmarket.csv"],
        #spark_binary="spark-submit",
        dag=dag

    )

     #---------------------------upload to minio
    @task
    def upload_tranform_minio():
        import boto3, sys

        minio_urls = [
            'memoire_5455b9-minio-1:9000',
            'http://172.18.0.2:9000'
            
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
        file_path = '/tmp/cleaned_data.csv'
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

    #--------------------------save to elasticsearch
    @task
    def upload_to_elasticsearch():
        """Upload cleaned data to Elasticsearch"""
        import pandas as pd
        from elasticsearch import Elasticsearch
        import json
        
        # Configuration Elasticsearch
        es_hosts = [
            'http://memoire_5455b9-elasticsearch-1:9200',
            'http://172.18.0.3:9200'
            
        ]
        
        es_client = None
        for host in es_hosts:
            try:
                logging.info(f"🔗 Tentative de connexion Elasticsearch: {host}")
                es_client = Elasticsearch([host])
                # Test de connexion
                es_client.info()
                logging.info(f"✅ Connexion Elasticsearch réussie: {host}")
                break
            except Exception as e:
                logging.warning(f"❌ Échec connexion Elasticsearch {host}: {e}")
                continue
        
        if es_client is None:
            logging.warning("⚠️ Elasticsearch non disponible - cette étape est optionnelle")
            logging.info("✅ Pipeline continue sans Elasticsearch")
            return True
        
        # Lire le fichier CSV nettoyé
        file_path = '/tmp/cleaned_data.csv'
        
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
            
            # Configuration de l'index
            index_name = 'coinmarket-data'
            
            # Créer l'index s'il n'existe pas
            if not es_client.indices.exists(index=index_name):
                mapping = {
                    "mappings": {
                        "properties": {
                            "price": {"type": "text"},
                            "type": {"type": "keyword"},
                            "Nombre_de_piece": {"type": "text"},
                            "Nombre_de_salle_bain": {"type": "text"},
                            "Superficie": {"type": "text"},
                            "category": {"type": "text"},
                            "area": {"type": "keyword"},
                            "city": {"type": "keyword"},
                        }
                    }
                }
                es_client.indices.create(index=index_name, body=mapping)
                logging.info(f"✅ Index '{index_name}' created")
            
            # Upload des données
            success_count = 0
            for idx, row in df.iterrows():
                try:
                    doc = row.to_dict()
                    # Nettoyer les valeurs NaN
                    doc = {k: v for k, v in doc.items() if pd.notna(v)}
                    
                    es_client.index(
                        index=index_name,
                        id=idx,
                        body=doc
                    )
                    success_count += 1
                except Exception as e:
                    logging.warning(f"⚠️ Error indexing document {idx}: {e}")
            
            logging.info(f"✅ Successfully indexed {success_count}/{len(df)} documents to Elasticsearch")
            return True
            
        except Exception as e:
            logging.warning(f"⚠️ Error uploading to Elasticsearch: {e}")
            logging.info("✅ Pipeline continue malgré l'erreur Elasticsearch")
            return True

    
    @task
    def es_health_check():

        es_hosts = [
            'http://memoire_5455b9-elasticsearch-1:9200',
            'http://172.18.0.3:9200',
            'http://elasticsearch:9200',
            'http://localhost:9200'
        ]

        last_error = None
        for host in es_hosts:
            try:
                logging.info(f"🔗 Test connexion Elasticsearch: {host}")
                es = Elasticsearch([host])
                es.info()  # health check
                logging.info(f"✅ Elasticsearch OK: {host}")
                return True
            except Exception as e:
                logging.warning(f"❌ ES indisponible sur {host}: {e}")
                last_error = e
                continue

        # si aucun host ne répond, on échoue (ou vous pouvez rendre optionnel si souhaité)
        raise RuntimeError(f"Elasticsearch indisponible ({last_error})")


    upload = upload_to_minio()
    verify_csv = verify_csv_exists()
    upload_transform = upload_tranform_minio()
    upload_elasticsearch = upload_to_elasticsearch()
    health = es_health_check()



    scraped >> upload >> verify_csv 

    verify_csv >> spark_job >> health >> [upload_transform, upload_elasticsearch]



    
   
