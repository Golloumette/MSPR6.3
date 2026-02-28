from fastapi import APIRouter
import crud, schemas
from .utils import add_crud_routes

router = APIRouter(prefix="/virus", tags=["virus"])

add_crud_routes(
    router,
    tags=["virus"],
    get_schema=schemas.Virus,
    create_schema=schemas.Virus,
    get_fn=crud.get_virus,
    create_fn=crud.create_virus,
)
