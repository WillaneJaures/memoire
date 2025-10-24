from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from database import SessionLocal
import query_helpers as helpers
import schemas

api_description = """
Bienvenue dans l'API de gestion des biens immobiliers.

Cette API permet de récupérer des informations sur les biens immobiliers.

### Fonctionnalités principales :
- Récupération de la liste des biens avec filtres
- Recherche par plage de prix ou superficie
- Statistiques par source, ville, type et catégorie
"""

app = FastAPI(
    title="API Immobilier",
    description=api_description,
    version="1.0.0",
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/", tags=["Santé"])
async def root():
    return {"message": "API immobilier opérationnelle"}

@app.get(
    "/properties", 
    response_model=List[schemas.RealEstate],
    tags=["Propriétés"],
    summary="Liste des propriétés"
)
def read_properties(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    type_filter: Optional[str] = Query(None, description="Filtrer par type"),
    area: Optional[str] = Query(None, description="Filtrer par quartier"),
    city: Optional[str] = Query(None, description="Filtrer par ville"),
    db: Session = Depends(get_db)
):
    """Récupère la liste des propriétés avec filtres optionnels"""
    return helpers.get_properties(db, skip, limit, type_filter, area, city)


@app.get(
    "/properties/search",
    response_model=List[schemas.RealEstate],
    tags=["Recherche"],
    summary="Recherche par area et type"
)
def search_properties(
    area: Optional[str] = Query(None, description="Zone géographique à filtrer"),
    type: Optional[str] = Query(None, description="Type de bien à filtrer"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """Recherche les propriétés par `area` et `type`. Retourne une liste de biens correspondants."""
    # Note: helpers.get_properties expects parameter name `type_filter`
    return helpers.get_properties(db, skip=skip, limit=limit, type_filter=type, area=area)

@app.get(
    "/properties/{property_id}",
    response_model=schemas.RealEstate,
    tags=["Propriétés"],
    summary="Détail d'une propriété"
)
def read_property(property_id: int, db: Session = Depends(get_db)):
    """Récupère les détails d'une propriété par son ID"""
    prop = helpers.get_property_by_id(db, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="Propriété non trouvée")
    return prop

@app.get(
    "/properties/price-range/",
    response_model=List[schemas.RealEstate],
    tags=["Recherche"],
    summary="Recherche par prix"
)
def properties_by_price(
    min_price: int = Query(..., description="Prix minimum"),
    max_price: int = Query(..., description="Prix maximum"),
    db: Session = Depends(get_db)
):
    """Récupère les propriétés dans une plage de prix"""
    return helpers.get_properties_by_price_range(db, min_price, max_price)

@app.get(
    "/properties/superficie-range/",
    response_model=List[schemas.RealEstate],
    tags=["Recherche"],
    summary="Recherche par superficie"
)
def properties_by_superficie(
    min_superficie: int = Query(..., description="Superficie minimum (m²)"),
    max_superficie: int = Query(..., description="Superficie maximum (m²)"),
    db: Session = Depends(get_db)
):
    """Récupère les propriétés dans une plage de superficie"""
    return helpers.get_properties_by_superficie_range(db, min_superficie, max_superficie)

@app.get(
    "/stats",
    response_model=schemas.Stats,
    tags=["Statistiques"],
    summary="Statistiques générales"
)
def read_stats(db: Session = Depends(get_db)):
    """Statistiques globales sur les propriétés"""
    return helpers.get_stats(db)

@app.get(
    "/stats/prices",
    response_model=schemas.PriceStats,
    tags=["Statistiques"],
    summary="Statistiques des prix"
)
def read_price_stats(db: Session = Depends(get_db)):
    """Prix moyens par type et catégorie"""
    return helpers.get_price_stats(db)



@app.get(
    "/analytics/annonces-par-source",
    tags=["Statistiques"],
    summary="Total d'annonces par source"
)
def annonces_par_source(db: Session = Depends(get_db)):
    """Renvoie le total d'annonces par source sous forme de liste de dicts {source, total}."""
    results = helpers.get_total_annonces_par_source(db)
    return results