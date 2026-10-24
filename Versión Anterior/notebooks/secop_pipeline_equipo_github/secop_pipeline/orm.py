from sqlalchemy import select
from sqlalchemy.orm import Session
from .db import get_engine
from .models import ContratoControl


def sample_contracts(limit=10):
    with Session(get_engine()) as session:
        return list(session.scalars(select(ContratoControl).order_by(ContratoControl.ircd.desc().nulls_last()).limit(limit)))


def sample_processes(limit=10):
    raise RuntimeError('El enfoque activo es contractual: use sample_contracts().')
