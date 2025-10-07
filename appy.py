# ------------------- Application CoinAfrique complète -------------------
import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd
import sqlite3
from typing import Optional
import plotly.express as px
import os
import docker
import tarfile
import io
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# ------------------- CONFIG -------------------
ES_HOST = st.secrets.get("ES_HOST", "http://localhost:9200")
ES_USER = st.secrets.get("ES_USER", None)
ES_PASS = st.secrets.get("ES_PASS", None)
INDEX = "coinmarket-data"
DB_PATH = "immobilier.db"
contenair_name = "memoire_5455b9-scheduler-1"
PAGE_SIZE = 10

# Configuration MinIO
MINIO_ENDPOINTS = [
    'http://memoire_5455b9-minio-1:9000',
    'http://172.18.0.2:9000'
]
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
MINIO_BUCKET = 'coinmarkettransform'
MINIO_OBJECT = 'cleaned_data.csv'

# Connexion Docker
client = docker.from_env()

# ------------------- MinIO -------------------
def get_minio_client():
    """Crée un client MinIO en essayant différents endpoints"""
    for endpoint in MINIO_ENDPOINTS:
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name='us-east-1'
            )
            # Test de connexion
            s3_client.list_buckets()
            return s3_client, endpoint
        except Exception as e:
            continue
    return None, None

# ------------------- ElasticSearch -------------------
def build_es_client(host: Optional[str] = None) -> Elasticsearch:
    host = host or ES_HOST
    kwargs = {"hosts": [host]}
    if ES_USER and ES_PASS:
        kwargs["basic_auth"] = (ES_USER, ES_PASS)
    return Elasticsearch(**kwargs)

@st.cache_resource
def get_es_client() -> Elasticsearch:
    es = build_es_client()
    es.info()
    return es

def build_query(text, city, type_, price_min, price_max, category):
    must_clauses, filter_clauses = [], []
    if text:
        must_clauses.append({
            "multi_match": {
                "query": text,
                "fields": ["Type^3", "category^2", "City", "Area"],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        })
    if city:
        filter_clauses.append({"match": {"City": {"query": city, "operator": "and"}}})
    if type_:
        filter_clauses.append({"match": {"Type": {"query": type_, "operator": "and"}}})
    if category:
        filter_clauses.append({"match": {"category": {"query": category, "operator": "and"}}})
    price_range = {}
    if price_min:
        try:
            price_range["gte"] = float(price_min)
        except:
            pass
    if price_max:
        try:
            price_range["lte"] = float(price_max)
        except:
            pass
    if price_range:
        filter_clauses.append({"range": {"price": price_range}})
    return {"bool": {"must": must_clauses or [{"match_all": {}}], "filter": filter_clauses}}

def search_es(es, query, page, page_size):
    from_ = max(page - 1, 0) * page_size
    resp = es.search(index=INDEX, query=query, from_=from_, size=page_size, track_total_hits=True)
    total = resp["hits"]["total"]["value"]
    hits = [h["_source"] for h in resp["hits"]["hits"]]
    return total, hits


# ------------------- SQLite -------------------
def init_database():
    """Initialise la base SQLite si nécessaire"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Type TEXT,
            category TEXT,
            City TEXT,
            Area TEXT,
            price REAL,
            Nombre_de_piece INTEGER,
            Nombre_de_salle_bain INTEGER,
            Superficie INTEGER
        )
    """)
    conn.commit()
    conn.close()


def load_csv_to_sqlite():
    """
    Récupère le fichier CSV transformé depuis le bucket MinIO coinmarkettransform,
    le charge dans un DataFrame et le sauvegarde dans SQLite.
    """
    try:
        # Connexion à MinIO
        s3_client, endpoint = get_minio_client()
        if s3_client is None:
            return False, "❌ Impossible de se connecter à MinIO avec aucun des endpoints"

        st.info(f"🔗 Connexion MinIO réussie avec: {endpoint}")

        # Vérifier si le bucket existe
        try:
            s3_client.head_bucket(Bucket=MINIO_BUCKET)
            st.info(f"✅ Bucket '{MINIO_BUCKET}' existe")
        except Exception as e:
            return False, f"❌ Bucket '{MINIO_BUCKET}' n'existe pas: {e}"

        # Lister les objets dans le bucket
        response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET)
        if 'Contents' in response:
            st.info(f"📁 Objets dans le bucket '{MINIO_BUCKET}':")
            for obj in response['Contents']:
                st.info(f"   📄 {obj['Key']} (taille: {obj['Size']} bytes)")
        else:
            return False, f"⚠️ Aucun objet trouvé dans le bucket '{MINIO_BUCKET}'"

        # Télécharger le fichier CSV depuis MinIO
        st.info(f"⬇️ Téléchargement de {MINIO_OBJECT} depuis MinIO...")
        
        # Utiliser get_object pour récupérer le contenu directement
        response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=MINIO_OBJECT)
        csv_content = response['Body'].read()
        
        # Lire le CSV directement depuis le contenu téléchargé
        from io import StringIO
        csv_string = csv_content.decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        
        st.info(f"✅ Fichier téléchargé et lu avec succès: {len(df)} lignes")

        # Nettoyage des données
        if "price" in df.columns:
            df["price"] = df["price"].astype(str).str.replace(r"[^\d.]", "", regex=True)
            df["price"] = pd.to_numeric(df["price"], errors="coerce")

        # Nettoyage éventuel des NaN ou doublons
        df.dropna(subset=["City", "category"], how="all", inplace=True)
        df.drop_duplicates(inplace=True)

        # Sauvegarde dans SQLite
        conn = sqlite3.connect(DB_PATH)
        df.to_sql("properties", conn, if_exists="replace", index=False)
        conn.commit()
        conn.close()

        return True, f"✅ {len(df):,} lignes importées depuis MinIO bucket '{MINIO_BUCKET}'"

    except Exception as e:
        return False, f"❌ Erreur pendant le chargement depuis MinIO: {e}"


def get_df_from_sqlite():
    """Lit la table properties depuis SQLite"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM properties", conn)
    conn.close()
    return df


# ======================================================================
# 🚀 INTERFACE STREAMLIT (2 onglets)
# ======================================================================
def main():
    st.set_page_config(page_title="CoinAfrique Analytics", layout="wide")
    st.title("🏠 CoinAfrique - Application complète")

    tab1, tab2 = st.tabs(["🔍 Moteur de recherche", "📊 Tableau de bord local"])

    # ======================================================
    # 🔍 Onglet 1 : Elasticsearch (inchangé)
    # ======================================================
    with tab1:
        st.header("🔍 Recherche dans Elasticsearch")

        text = st.text_input("Recherche texte", placeholder="ex: appartement Dakar 3 pièces")
        city = st.text_input("Ville")
        type_ = st.text_input("Type de bien (Appartement, Villa...)")
        category = st.selectbox("Catégorie", ["", "Vente", "Location"], index=0)
        c1, c2 = st.columns(2)
        with c1:
            price_min = st.text_input("Prix minimum")
        with c2:
            price_max = st.text_input("Prix maximum")

        page = st.number_input("Page", min_value=1, value=1)
        run = st.button("Rechercher dans Elasticsearch")

        if run:
            try:
                es = get_es_client()
                query = build_query(text, city, type_, price_min, price_max, category)
                total, results = search_es(es, query, page, PAGE_SIZE)
                st.success(f"{total} résultats trouvés")

                df = pd.DataFrame(results)
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Erreur de recherche : {e}")

    # ======================================================
    # 📊 Onglet 2 : Dashboard basé sur données MinIO
    # ======================================================
    with tab2:
        st.header("📊 Tableau de bord (basé sur les données MinIO coinmarkettransform)")

        if st.button("📥 Charger les données depuis MinIO dans SQLite"):
            init_database()
            success, msg = load_csv_to_sqlite()
            if success:
                st.success(msg)
            else:
                st.error(msg)

        try:
            df = get_df_from_sqlite()
        except Exception as e:
            st.error(f"Erreur de lecture SQLite : {e}")
            return

        if df.empty:
            st.info("⚠️ Aucune donnée trouvée. Clique sur le bouton ci-dessus pour charger les données depuis MinIO.")
            return

        # ---- KPIs ----
        prix_col = next((col for col in df.columns if "price" in col.lower()), None)
        city_col = next((col for col in df.columns if "city" in col.lower()), None)

        col1, col2, col3 = st.columns(3)
        col1.metric("Total annonces", f"{len(df):,}")
        #if prix_col:
        #    prix_moyen = df[prix_col].mean(skipna=True)
        #    col2.metric("Prix moyen", f"{prix_moyen:,.0f} FCFA")
        #else:
        #    col2.metric("Prix moyen", "N/A")

        if city_col:
            col3.metric("Nombre de villes", f"{df[city_col].nunique()}")
        else:
            col3.metric("Nombre de villes", "N/A")

        st.markdown("---")

        # ---- Graphiques ----
        if prix_col:
            fig1 = px.histogram(df, x=prix_col, nbins=30, title="Distribution des prix")
            st.plotly_chart(fig1, use_container_width=True)

        if "category" in df.columns:
            fig2 = px.pie(df, names="category", title="Répartition Vente/Location")
            st.plotly_chart(fig2, use_container_width=True)

        if city_col and prix_col:
            df_group = df.groupby(city_col)[prix_col].mean().sort_values(ascending=False).head(10)
            fig3 = px.bar(df_group, title="Top 10 villes par prix moyen")
            st.plotly_chart(fig3, use_container_width=True)

        st.markdown("### 📋 Données brutes")
        st.dataframe(df, use_container_width=True)
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("📤 Télécharger CSV", csv, "immobilier.csv", "text/csv")


# ------------------- MAIN EXECUTION -------------------
if __name__ == "__main__":
    main()
