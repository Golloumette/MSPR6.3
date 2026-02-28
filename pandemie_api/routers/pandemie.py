from fastapi import APIRouter
import crud, schemas
from .utils import add_crud_routes

router = APIRouter(prefix="/pandemies", tags=["pandemies"])

add_crud_routes(
    router,
    tags=["pandemies"],
    get_schema=schemas.Pandemie,
    create_schema=schemas.Pandemie,
    get_fn=crud.get_pandemies,
    create_fn=crud.create_pandemie,
)
