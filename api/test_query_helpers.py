from database import SessionLocal
from query_helpers import *


db = SessionLocal()


#test de la fonction get_properties avec filtrage par type
properties = get_properties(db, skip=0, limit=5, type="Appartements")
print("\nFirst 5 properties of type 'Appartements':")
for prop in properties:
    print(prop.id, prop.price, prop.type, prop.superficie, prop.nombre_sdb)


#test de la fonction get_properties_by_price_range
properties_in_range = get_properties_by_price_range(db, min_price=500000, max_price=1000000, skip=0, limit=5)
print("\nFirst 5 properties with price between 500000 and 1000000:")
for prop in properties_in_range:
    print(prop.id, prop.price, prop.type, prop.superficie, prop.nombre_sdb)


#test de la fonction get_properties_by_superficie
properties_by_area = get_properties_by_superficie(db, min_superficie=50, max_superficie=100, skip=0, limit=5)
print("\nFirst 5 properties with area between 50 and 100:")
for prop in properties_by_area:
    print(prop.price, prop.type, prop.superficie, prop.nombre_sdb)

#test de la fonction get_nombre_anonces_par_source
nombre_anonces = get_nombre_anonces_par_source(db)
print("\nNombre d'annonces par source:")
for source, count in nombre_anonces:
    print(source, count)

#test de la fonction get_prix_moyen_par_type_et_category
prix_moyen = get_prix_moyen_par_type_et_category(db)
print("\nPrix moyen par type et category:")
for type, category, avg_price in prix_moyen:
    print(type, category, avg_price)


