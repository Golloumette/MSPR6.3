from fastapi import APIRouter
import crud, schemas
from .utils import add_crud_routes

router = APIRouter(prefix="/logs", tags=["logs"])

add_crud_routes(
    router,
    tags=["logs"],
    get_schema=schemas.LoggingInsert,
    create_schema=schemas.LoggingInsert,
    get_fn=crud.get_logs,
    create_fn=crud.create_log,
)
