import sqlite3
import pandas as pd

# Connexion
conn = sqlite3.connect('./immobilier.db')

# Lire toutes les données
df = pd.read_sql_query("SELECT * FROM realestate limit 5", conn)

print(f"Total: {len(df)} propriétés")
print("\nAperçu:")
print(df.head())

print("\nPar ville:")
print(df.groupby('city')['price'].agg(['count', 'mean']).sort_values('count', ascending=False))

print("\nPar source:")
print(df.groupby('source').size())

conn.close()