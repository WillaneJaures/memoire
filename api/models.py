"""SQLAlchemy models"""
from sqlalchemy import Column, Integer, String, Float, ForeignKey 
from sqlalchemy.orm import relationship # permet des relations de clé étrangère entre les tables.
from database import Base

class RealEstate(Base):
    __tablename__ = "realestate"

    id = Column(Integer, primary_key=True, index=True)
    price = Column(Integer, index=True)
    type = Column(String, index=True)
    superficie = Column(Integer, index=True)
    nombre_chambres = Column(Integer, index=True)
    nombre_sdb = Column(Integer, index=True)
    category = Column(String, index=True)
    area = Column(String, index=True)
    city = Column(String, index=True)
    source = Column(String, index=True)
    