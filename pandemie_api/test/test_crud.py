import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker 
from database import Base
import crud
import schemas
import models

# Base de données en mémoire pour les tests
engine = create_engine("sqlite:///:memory:")
TestingSessionLocal = sessionmaker(bind=engine)

@pytest.fixture(scope="function")
def db():
    Base.metadata.create_all(bind=engine)
    session = TestingSessionLocal()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)

def test_create_continent(db):
    continent = schemas.ContinentCreate(nom_continent="Europe")
    db_cont = crud.create_continent(db, continent)
    assert db_cont.nom_continent == "Europe"


def test_generic_get_all_and_create(db):
    # create a country and retrieve via generic helpers
    pays = schemas.PaysCreate(nom="France", code_lettre="FR", continent_id=1)
    created = crud.create_item(db, models.Pays, pays)
    assert created.nom == "France"
    all_pays = crud.get_all(db, models.Pays)
    assert len(all_pays) == 1 and all_pays[0].code_lettre == "FR"
