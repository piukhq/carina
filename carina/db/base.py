# Import all the models, so that Base has them before being
# imported by Alembic
from carina.db.base_class import Base  # noqa
from carina.models import *  # noqa
