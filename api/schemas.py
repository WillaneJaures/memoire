from pydantic import BaseModel, ConfigDict
from typing import Optional

class RealEstateBase(BaseModel):
    price: Optional[int] = None
    type: Optional[str] = None
    superficie: Optional[int] = None
    nombre_chambres: Optional[int] = None
    nombre_sdb: Optional[int] = None
    category: Optional[str] = None
    area: Optional[str] = None
    city: Optional[str] = None
    source: Optional[str] = None

class RealEstate(RealEstateBase):
    id: int
    
    
    model_config = ConfigDict(from_attributes=True)

class RealEstateCreate(RealEstateBase):
    pass

class Stats(BaseModel):
    total: int
    by_source: dict
    by_city: dict
    avg_price: float
    
    model_config = ConfigDict(from_attributes=True)

class PriceStats(BaseModel):
    avg_price_by_type: dict
    avg_price_by_category: dict
    
    model_config = ConfigDict(from_attributes=True)