from sqlalchemy.orm import Session
from sqlalchemy import func
import models

def get_properties(
    db: Session, 
    skip: int = 0, 
    limit: int = 100, 
    type_filter: str = None,
    area: str = None,
    city: str = None
):
    query = db.query(models.RealEstate)
    
    if type_filter:
        query = query.filter(models.RealEstate.type == type_filter)
    
    if area:
        query = query.filter(models.RealEstate.area.ilike(f"%{area}%"))
    
    if city:
        query = query.filter(models.RealEstate.city.ilike(f"%{city}%"))
    
    return query.offset(skip).limit(limit).all()

def get_property_by_id(db: Session, property_id: int):
    return db.query(models.RealEstate).filter(
        models.RealEstate.id == property_id
    ).first()

def get_properties_by_price_range(db: Session, min_price: int, max_price: int):
    return db.query(models.RealEstate).filter(
        models.RealEstate.price >= min_price,
        models.RealEstate.price <= max_price
    ).all()

def get_properties_by_superficie_range(db: Session, min_superficie: int, max_superficie: int):
    return db.query(models.RealEstate).filter(
        models.RealEstate.superficie >= min_superficie,
        models.RealEstate.superficie <= max_superficie
    ).all()

def get_stats(db: Session):
    total = db.query(func.count(models.RealEstate.id)).scalar()
    avg_price = db.query(func.avg(models.RealEstate.price)).scalar()
    
    by_source = db.query(
        models.RealEstate.source,
        func.count(models.RealEstate.id)
    ).group_by(models.RealEstate.source).all()
    
    by_city = db.query(
        models.RealEstate.city,
        func.count(models.RealEstate.id)
    ).group_by(models.RealEstate.city).all()
    
    return {
        'total': total or 0,
        'avg_price': float(avg_price) if avg_price else 0.0,
        'by_source': {source: count for source, count in by_source},
        'by_city': {city: count for city, count in by_city}
    }

def get_total_annonces_par_source(db: Session):
    """Retourne le nombre total d'annonces par source sous forme de liste de dicts.

    Exemple: [ {'source': 'siteA', 'total': 123}, {'source': 'siteB', 'total': 45} ]
    """
    results = db.query(
        models.RealEstate.source,
        func.count(models.RealEstate.id)
    ).group_by(models.RealEstate.source).all()

    return [{'source': source or 'unknown', 'total': int(count)} for source, count in results]

def get_price_stats(db: Session):
    avg_by_type = db.query(
        models.RealEstate.type,
        func.avg(models.RealEstate.price)
    ).group_by(models.RealEstate.type).all()
    
    avg_by_category = db.query(
        models.RealEstate.category,
        func.avg(models.RealEstate.price)
    ).group_by(models.RealEstate.category).all()
    
    return {
        'avg_price_by_type': {t: float(p) for t, p in avg_by_type if p},
        'avg_price_by_category': {c: float(p) for c, p in avg_by_category if p}
    }

