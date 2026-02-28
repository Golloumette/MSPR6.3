from fastapi import APIRouter
import crud, schemas
from .utils import add_crud_routes

router = APIRouter(prefix="/pays", tags=["pays"])

add_crud_routes(
    router,
    tags=["pays"],
    get_schema=schemas.Pays,
    create_schema=schemas.Pays,
    get_fn=crud.get_pays,
    create_fn=crud.create_pays,
)
