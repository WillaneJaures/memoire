# ------------------- Application CoinAfrique complète -------------------
import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd
import sqlite3
from typing import Optional
import plotly.express as px
import os
from typing import Optional
import glob

# ------------------- CONFIG -------------------
ES_HOST = st.secrets.get("ES_HOST", "http://localhost:9200")
ES_USER = st.secrets.get("ES_USER", None)
ES_PASS = st.secrets.get("ES_PASS", None)
INDEX = "coinmarket-data"
DB_PATH = "immobilier.db"
CSV_PATH = "/tmp/coinmarket.csv"
PAGE_SIZE = 10

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
    Charge automatiquement tous les fichiers CSV générés par PySpark
    depuis le répertoire /tmp/cleaned_data.csv et les enregistre dans SQLite.
    """
    folder_path = "/tmp/coinmarket.csv"

    # Vérifie si le dossier existe
    if not os.path.exists(folder_path):
        return False, f"❌ Dossier introuvable : {folder_path}"

    # Récupère tous les fichiers part-*.csv
    csv_files = glob.glob(os.path.join(folder_path, "part-*.csv"))
    if not csv_files:
        return False, f"❌ Aucun fichier 'part-*.csv' trouvé dans {folder_path}"

    st.info(f"📄 {len(csv_files)} fichier(s) CSV détecté(s). Chargement en cours...")

    try:
        # Lecture et fusion des CSV
        dfs = []
        for file in csv_files:
            df_part = pd.read_csv(file)
            if not df_part.empty:
                dfs.append(df_part)
        if not dfs:
            return False, "❌ Les fichiers CSV sont vides."
        df = pd.concat(dfs, ignore_index=True)

        # Conversion et nettoyage optionnel
        if "price" in df.columns:
            df["price"] = pd.to_numeric(df["price"], errors="coerce")

        # Enregistrement dans SQLite
        conn = sqlite3.connect(DB_PATH)
        df.to_sql("properties", conn, if_exists="replace", index=False)
        conn.commit()
        conn.close()

        return True, f"✅ {len(df):,} lignes importées depuis {len(csv_files)} fichier(s) Spark vers SQLite."

    except Exception as e:
        return False, f"❌ Erreur pendant le chargement : {e}"
    

def get_df_from_sqlite():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM properties", conn)
    conn.close()
    return df


# ======================================================================
# 🚀 INTERFACE STREAMLIT (2 onglets indépendants)
# ======================================================================

def main():
    st.set_page_config(page_title="CoinAfrique Analytics", layout="wide")
    st.title("🏠 CoinAfrique - Application complète")

    tab1, tab2 = st.tabs(["🔍 Moteur de recherche", "📊 Tableau de bord local"])

    # ======================================================
    # 🔍 Onglet 1 : Recherche Elasticsearch
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
    # 📊 Onglet 2 : Dashboard basé sur CSV local
    # ======================================================
    with tab2:
        st.header("📊 Tableau de bord (basé sur /tmp/cleaned_data.csv)")

        if st.button("📥 Charger les données dans SQLite"):
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
            st.info("⚠️ Aucune donnée trouvée. Clique sur le bouton pour charger le CSV.")
            return

        # ---- KPIs ----
        col1, col2, col3 = st.columns(3)
        col1.metric("Total annonces", f"{len(df):,}")
        col2.metric("Prix moyen", f"{df['price'].mean():,.0f} FCFA")
        col3.metric("Nombre de villes", f"{df['City'].nunique()}")

        st.markdown("---")

        # ---- Graphiques ----
        col1, col2 = st.columns(2)
        with col1:
            fig1 = px.histogram(df, x="price", nbins=30, title="Distribution des prix")
            st.plotly_chart(fig1, use_container_width=True)
        with col2:
            if "category" in df.columns:
                fig2 = px.pie(df, names="category", title="Répartition Vente/Location")
                st.plotly_chart(fig2, use_container_width=True)

        if "City" in df.columns and "price" in df.columns:
            fig3 = px.bar(
                df.groupby("City")["price"].mean().sort_values(ascending=False).head(10),
                title="Top 10 villes par prix moyen"
            )
            st.plotly_chart(fig3, use_container_width=True)

        st.markdown("### 📋 Données brutes")
        st.dataframe(df, use_container_width=True)
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("📤 Télécharger CSV", csv, "immobilier.csv", "text/csv")

# ------------------- MAIN EXECUTION -------------------
if __name__ == "__main__":
    main()