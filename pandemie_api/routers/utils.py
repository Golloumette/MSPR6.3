from fastapi import Depends
from sqlalchemy.orm import Session
from database import get_db
from .security import admin_required


def add_crud_routes(
    router,
    tags: list,
    get_schema,
    create_schema,
    get_fn,
    create_fn,
    admin: bool = False,
):
    """Ajoute sur un router deux routes GET/POST standard.

    router : APIRouter déjà instancié (avec prefix/tags facultatifs).
    tags : liste des tags de l'API.
    get_schema : pydantic model de sortie pour GET list.
    create_schema : pydantic model pour POST body et retour.
    get_fn / create_fn : fonctions crud(db, obj?)
    admin : si True, la route POST exige un utilisateur admin.
    """
    # on suppose que router.prefix/tags sont déjà définis par l'appelant

    @router.get("/", response_model=list[get_schema])
    def read_all(db: Session = Depends(get_db)):
        return get_fn(db)

    if admin:
        @router.post("/", response_model=create_schema)
        def create(
            item: create_schema,
            db: Session = Depends(get_db),
            user=Depends(admin_required),
        ):
            return create_fn(db, item)
    else:
        @router.post("/", response_model=create_schema)
        def create(item: create_schema, db: Session = Depends(get_db)):
            return create_fn(db, item)
