import boto3
import pandas as pd
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#----------------------------------minio connection----------------------------------
minio_url = 'http://172.18.0.3:9000'

s3_client = None
try:
    logging.info(f"Tentative de connexion à {minio_url}")
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_url,
        aws_access_key_id='minio',
        aws_secret_access_key='minio123',
        region_name='us-east-1'
    )
    
    # Test de connexion
    buckets = s3_client.list_buckets()
    logging.info(f"✅ Connexion réussie avec: {minio_url}")
    logging.info(f"Buckets disponibles: {[b['Name'] for b in buckets['Buckets']]}")
    
except Exception as e:
    logging.error(f"❌ Échec avec {minio_url}: {e}")
    
    # Vérification finale
    if s3_client is None:
        raise ConnectionError("❌ Impossible de se connecter à MinIO")

#---------------get coinmarket-transform data and display with pandas---------------
bucket_name = 'coinmarkettransform'
object_name = 'cleaned_data.csv'

try:
    logging.info(f"📦 Accès au bucket: {bucket_name}")
    
    # Vérifier si le bucket existe
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"✅ Bucket '{bucket_name}' existe")
    except Exception as e:
        logging.error(f"❌ Bucket '{bucket_name}' n'existe pas: {e}")
        raise
    
    # Lister les objets dans le bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        logging.info(f"📁 Objets dans le bucket '{bucket_name}':")
        for obj in response['Contents']:
            logging.info(f"   📄 {obj['Key']} (taille: {obj['Size']} bytes)")
    else:
        logging.warning(f"⚠️ Aucun objet trouvé dans le bucket '{bucket_name}'")
    
    # Télécharger le fichier CSV
    logging.info(f"⬇️ Téléchargement de {object_name}...")
    s3_client.download_file(bucket_name, object_name, f'/tmp/{object_name}')
    logging.info(f"✅ Fichier téléchargé vers /tmp/{object_name}")
    
    # Lire et afficher les données avec pandas
    logging.info("📊 Lecture des données avec pandas...")
    df = pd.read_csv(f'/tmp/{object_name}')
    
    print("\n" + "="*80)
    print(f"📊 DONNÉES DU BUCKET '{bucket_name}' - FICHIER '{object_name}'")
    print("="*80)
    
    print(f"\n📈 Informations générales:")
    print(f"   • Nombre de lignes: {len(df)}")
    print(f"   • Nombre de colonnes: {len(df.columns)}")
    print(f"   • Colonnes: {list(df.columns)}")
    
    print(f"\n📋 Aperçu des données (5 premières lignes):")
    print(df.head().to_string(index=False))
    
    print(f"\n📊 Statistiques descriptives:")
    print(df.describe().to_string())
    
    print(f"\n🔍 Types de données:")
    print(df.dtypes.to_string())
    
    print(f"\n❓ Valeurs manquantes par colonne:")
    missing_values = df.isnull().sum()
    for col, missing in missing_values.items():
        if missing > 0:
            print(f"   • {col}: {missing} valeurs manquantes")
        else:
            print(f"   • {col}: Aucune valeur manquante")
    
    print("\n" + "="*80)
    print("valeur unique dans price:")
    print(df['price'].unique())
    
except Exception as e:
    logging.error(f"❌ Erreur lors de l'accès aux données: {e}")
    logging.error(f"❌ Détails de l'erreur: {type(e).__name__}: {str(e)}")