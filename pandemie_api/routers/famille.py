from fastapi import APIRouter
import crud, schemas
from .utils import add_crud_routes

router = APIRouter(prefix="/familles", tags=["familles"])

add_crud_routes(
    router,
    tags=["familles"],
    get_schema=schemas.Famille,
    create_schema=schemas.Famille,
    get_fn=crud.get_familles,
    create_fn=crud.create_famille,
)
