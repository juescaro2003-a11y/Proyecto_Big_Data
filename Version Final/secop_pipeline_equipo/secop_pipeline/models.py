from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Float, SmallInteger, String, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class ProcesoAnalitico(Base):
    """Mapeo ORM de las columnas principales de gold.procesos_modelo.

    La tabla física tiene más columnas; el ORM puede mapear solo las necesarias
    para las consultas de la aplicación.
    """
    __tablename__ = "procesos_modelo"
    __table_args__ = {"schema": "gold"}

    id_del_proceso: Mapped[str] = mapped_column(String, primary_key=True)
    id_del_portafolio: Mapped[Optional[str]] = mapped_column(String)
    nit_entidad: Mapped[Optional[str]] = mapped_column(String)
    entidad: Mapped[Optional[str]] = mapped_column(String)
    modalidad_de_contratacion: Mapped[Optional[str]] = mapped_column(String)
    tipo_de_contrato: Mapped[Optional[str]] = mapped_column(String)
    precio_base: Mapped[Optional[float]] = mapped_column(Float)
    fecha_de_publicacion_del: Mapped[Optional[datetime]] = mapped_column(DateTime)
    objetivo: Mapped[Optional[int]] = mapped_column(SmallInteger)

class Contrato(Base):
    __tablename__ = "contratos"
    __table_args__ = {"schema": "secop"}

    id_contrato: Mapped[str] = mapped_column(String, primary_key=True)
    proceso_de_compra: Mapped[Optional[str]] = mapped_column(String)
    nit_entidad: Mapped[Optional[str]] = mapped_column(String)
    documento_proveedor: Mapped[Optional[str]] = mapped_column(String)
    valor_del_contrato: Mapped[Optional[float]] = mapped_column(Float)

class Proveedor(Base):
    __tablename__ = "proveedores"
    __table_args__ = {"schema": "secop"}

    codigo: Mapped[str] = mapped_column(String, primary_key=True)
    nit: Mapped[Optional[str]] = mapped_column(String)
    departamento: Mapped[Optional[str]] = mapped_column(String)
    tipo_empresa: Mapped[Optional[str]] = mapped_column(String)
    espyme: Mapped[Optional[str]] = mapped_column(String)


class ContratoControl(Base):
    """Una fila contractual. Scores e IRCD pueden ser nulos por falta de evidencia."""
    __tablename__ = 'contratos_control'
    __table_args__ = {'schema': 'gold'}
    id_contrato: Mapped[str] = mapped_column(String, primary_key=True)
    entidad_id: Mapped[Optional[str]] = mapped_column(String)
    proveedor_id: Mapped[Optional[str]] = mapped_column(String)
    fecha_de_firma: Mapped[Optional[datetime]] = mapped_column(DateTime)
    score_concentracion: Mapped[Optional[int]] = mapped_column(SmallInteger)
    score_recurrencia: Mapped[Optional[int]] = mapped_column(SmallInteger)
    score_valor_atipico: Mapped[Optional[int]] = mapped_column(SmallInteger)
    ircd: Mapped[Optional[int]] = mapped_column(SmallInteger)
    estado_indice: Mapped[str] = mapped_column(String)
    nivel_revision: Mapped[str] = mapped_column(String)
