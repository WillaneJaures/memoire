##Database configuration

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

SQLALCHEMY_DATABASE_URL = "sqlite:///../data/immobilier.db"

##cree moteur de base de donnees qui etablit la connexion a la base de donnees
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

#definir session locale, qui permet de creer des sesseions pour interagir avec la base de donnees
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#base de donnees declarative
Base = declarative_base()
'''
if __name__ == "__main__":
    try:
        # Test the connection
        with engine.connect() as conn:
            print("Database connection successful.")
    except Exception as e:
        print(f"Database connection failed: {e}")'''