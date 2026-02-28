from fastapi import APIRouter
import crud, schemas
from .utils import add_crud_routes

router = APIRouter(prefix="/continents", tags=["continents"])

# simple CRUD routes with admin restriction on POST
add_crud_routes(
    router,
    tags=["continents"],
    get_schema=schemas.Continent,
    create_schema=schemas.Continent,
    get_fn=crud.get_continents,
    create_fn=crud.create_continent,
    admin=True,
)
