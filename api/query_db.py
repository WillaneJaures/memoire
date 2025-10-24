import sqlite3
import pandas as pd
import logging

# Connexion
try:
    conn = sqlite3.connect('./data/immobilier.db')
    logging.info("Connexion réussie à la base de données SQLite")
    df = pd.read_sql_query("SELECT * FROM realestate limit 5", conn)

    print(f"Total: {len(df)} propriétés")
    print("\nAperçu:")
    print(df.head())
    #print(df.head(10))
except sqlite3.Error as e:
    logging.error(f"Erreur lors de la connexion à la base de données SQLite: {e}")
    


    conn.close()