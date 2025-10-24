from database import SessionLocal
from models import RealEstate

db = SessionLocal()

immo = db.query(RealEstate).limit(10).all()

'''for item in immo:
    print(item.id, item.price, item.type, item.superficie, item.nombre_chambres, item.nombre_sdb, item.category, item.area, item.city, item.source)
'''
#recuperons les biens de type appartements
appartements = db.query(RealEstate).filter(RealEstate.type == "Appartements").limit(10).all()
print("\nAppartements:")
for app in appartements:
    print(app.id, app.price, app.type, app.superficie, app.nombre_chambres, app.nombre_sdb, app.category, app.area, app.city, app.source)

#recuperons les bien de prix 1000000
bien_cher = db.query(RealEstate).filter(RealEstate.price >= 1000000).limit(5).all()
print("\nBiens de prix superieur ou egal a 1000000:")
for bien in bien_cher:
    print(bien.id, bien.price, bien.type, bien.superficie, bien.nombre_chambres, bien.nombre_sdb, bien.category, bien.area, bien.city, bien.source) 

#bien de type appartement et de prix inferieur ou egal a 500000
appart_pas_cher = db.query(RealEstate).filter(RealEstate.type == "Appartements", RealEstate.price >= 500000).limit(5).all()
print("\nAppartements de prix inferieur ou egal a 500000:")
for app in appart_pas_cher:
    print(app.id, app.price, app.type, app.superficie, app.nombre_chambres, app.nombre_sdb, app.category, app.area, app.city, app.source)