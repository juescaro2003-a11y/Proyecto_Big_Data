from __future__ import annotations

import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
OUTPUT_DIR = ROOT / "salida"

for p in (BRONZE_DIR, SILVER_DIR, GOLD_DIR, OUTPUT_DIR):
    p.mkdir(parents=True, exist_ok=True)

PORTAL = "https://www.datos.gov.co/resource"

START_DATE = pd.Timestamp(os.getenv("START_DATE", "2017-01-01"))
CUTOFF_DATE = pd.Timestamp(os.getenv("CUTOFF_DATE") or pd.Timestamp.today().normalize())
BATCH_SIZE = int(os.getenv("SOCRATA_BATCH_SIZE", "50000"))
APP_TOKEN = os.getenv("DATA_GOV_APP_TOKEN", "").strip() or None

BASES = {
    "procesos": {
        "codigo": "p6dx-8zbt",
        "fecha": "fecha_de_publicacion_del",
        "particion": "mensual",
    },
    "contratos": {
        "codigo": "jbjy-vk9h",
        "fecha": "fecha_de_firma",
        "particion": "mensual",
    },
    "proveedores": {
        "codigo": "qmzu-gj57",
        "fecha": "fecha_creacion",
        "particion": "catalogo",
    },
    "grupos": {
        "codigo": "ceth-n4bn",
        "fecha": "fecha_creaci_n_grupo",
        "particion": "catalogo",
    },
}

# Basado en el notebook original. En Procesos se añade id_del_portafolio desde el
# inicio para evitar una segunda consulta posterior solo para construir el puente
# Procesos <-> Contratos.
COLUMNAS = {
    "procesos": [
        "id_del_proceso", "id_del_portafolio",
        "nit_entidad", "entidad", "codigo_entidad",
        "departamento_entidad", "ciudad_entidad", "ordenentidad",
        "modalidad_de_contratacion", "tipo_de_contrato", "subtipo_de_contrato",
        "codigo_principal_de_categoria", "fase", "duracion",
        "unidad_de_duracion", "precio_base", "numero_de_lotes",
        "fecha_de_publicacion_del", "fecha_de_recepcion_de", "fecha_adjudicacion",
        "proveedores_invitados", "proveedores_con_invitacion",
        "visualizaciones_del", "proveedores_que_manifestaron",
        "respuestas_al_procedimiento", "respuestas_externas",
        "conteo_de_respuestas_a_ofertas", "proveedores_unicos_con",
        "adjudicado", "estado_del_procedimiento", "estado_resumen",
        "nit_del_proveedor_adjudicado", "codigoproveedor",
        "valor_total_adjudicacion", "departamento_proveedor", "ciudad_proveedor",
    ],
    "contratos": [
        "id_contrato", "proceso_de_compra", "nit_entidad", "nombre_entidad",
        "codigo_entidad", "documento_proveedor", "codigo_proveedor",
        "departamento", "ciudad", "orden", "sector", "rama",
        "entidad_centralizada", "es_grupo", "es_pyme",
        "estado_contrato", "codigo_de_categoria_principal", "tipo_de_contrato",
        "modalidad_de_contratacion", "origen_de_los_recursos", "destino_gasto",
        "habilita_pago_adelantado", "liquidaci_n", "espostconflicto",
        "fecha_de_firma", "fecha_de_inicio_del_contrato",
        "fecha_de_fin_del_contrato", "duraci_n_del_contrato",
        "dias_adicionados", "ultima_actualizacion",
        "valor_del_contrato", "valor_facturado", "valor_pagado",
        "valor_pendiente_de_pago", "valor_amortizado",
        "valor_pendiente_de_ejecucion", "saldo_cdp", "saldo_vigencia",
    ],
    "proveedores": [
        "codigo", "nit", "es_entidad", "es_grupo", "esta_activa",
        "fecha_creacion", "codigo_categoria_principal",
        "descripcion_categoria_principal", "pais", "departamento", "municipio",
        "tipo_empresa", "espyme", "ubicacion", "tama_o_entidad",
    ],
    "grupos": [
        "codigo_grupo", "nit_grupo", "es_entidad", "es_proveedor",
        "esta_activo", "fecha_creaci_n_grupo",
        "codigo_categor_a_principal_grupo", "pais_grupo", "departamento_grupo",
        "minucipio", "tipo_empresa_grupo", "es_mipyme", "ubicaci_n_grupo",
        "codigo_participante", "nit_participante", "tipo_empresa_participante",
        "ubicaci_n_participante", "fecha_creaci_n_participante",
        "participacion", "es_lider_del_grupo",
    ],
}

TIPOS = {
    "procesos": {
        "llave": [
            "id_del_proceso", "id_del_portafolio", "nit_entidad", "codigo_entidad",
            "nit_del_proveedor_adjudicado", "codigoproveedor",
        ],
        "etiqueta": [
            "entidad", "departamento_entidad", "ciudad_entidad", "ordenentidad",
            "modalidad_de_contratacion", "tipo_de_contrato", "subtipo_de_contrato",
            "codigo_principal_de_categoria", "fase", "unidad_de_duracion",
            "adjudicado", "estado_del_procedimiento", "estado_resumen",
            "departamento_proveedor", "ciudad_proveedor",
        ],
        "numero": [
            "duracion", "precio_base", "numero_de_lotes", "proveedores_invitados",
            "proveedores_con_invitacion", "visualizaciones_del",
            "proveedores_que_manifestaron", "respuestas_al_procedimiento",
            "respuestas_externas", "conteo_de_respuestas_a_ofertas",
            "proveedores_unicos_con", "valor_total_adjudicacion",
        ],
        "fecha": [
            "fecha_de_publicacion_del", "fecha_de_recepcion_de", "fecha_adjudicacion",
        ],
    },
    "contratos": {
        "llave": [
            "id_contrato", "proceso_de_compra", "nit_entidad", "codigo_entidad",
            "documento_proveedor", "codigo_proveedor", "duraci_n_del_contrato",
        ],
        "etiqueta": [
            "nombre_entidad", "departamento", "ciudad", "orden", "sector", "rama",
            "entidad_centralizada", "es_grupo", "es_pyme", "estado_contrato",
            "codigo_de_categoria_principal", "tipo_de_contrato",
            "modalidad_de_contratacion", "origen_de_los_recursos", "destino_gasto",
            "habilita_pago_adelantado", "liquidaci_n", "espostconflicto",
        ],
        "numero": [
            "dias_adicionados", "valor_del_contrato", "valor_facturado",
            "valor_pagado", "valor_pendiente_de_pago", "valor_amortizado",
            "valor_pendiente_de_ejecucion", "saldo_cdp", "saldo_vigencia",
        ],
        "fecha": [
            "fecha_de_firma", "fecha_de_inicio_del_contrato",
            "fecha_de_fin_del_contrato", "ultima_actualizacion",
        ],
    },
    "proveedores": {
        "llave": ["codigo", "nit"],
        "etiqueta": [
            "es_entidad", "es_grupo", "esta_activa", "codigo_categoria_principal",
            "descripcion_categoria_principal", "pais", "departamento", "municipio",
            "tipo_empresa", "espyme", "ubicacion", "tama_o_entidad",
        ],
        "numero": [],
        "fecha": ["fecha_creacion"],
    },
    "grupos": {
        "llave": ["codigo_grupo", "nit_grupo", "codigo_participante", "nit_participante"],
        "etiqueta": [
            "es_entidad", "es_proveedor", "esta_activo",
            "codigo_categor_a_principal_grupo", "pais_grupo", "departamento_grupo",
            "minucipio", "tipo_empresa_grupo", "es_mipyme", "ubicaci_n_grupo",
            "tipo_empresa_participante", "ubicaci_n_participante",
            "es_lider_del_grupo",
        ],
        "numero": ["participacion"],
        "fecha": ["fecha_creaci_n_grupo", "fecha_creaci_n_participante"],
    },
}

# Verificados contra api/views/jbjy-vk9h.json el 2026-10-07.
CONTROL_OPTIONAL_COLUMNS = ['tipodocproveedor', 'proveedor_adjudicado',
    'justificacion_modalidad_de', 'descripcion_del_proceso', 'objeto_del_contrato',
    'urlproceso', 'referencia_del_contrato']
COLUMNAS['contratos'].extend(CONTROL_OPTIONAL_COLUMNS)
TIPOS['contratos']['etiqueta'].extend(CONTROL_OPTIONAL_COLUMNS)

VACIOS = {
    "no definido", "no definida", "no provisto",
    "n/a", "na", "sin dato", "no informado",
    "-", ".", "null", "none", "0001-01-01t00:00:00.000", "",
}

# Constantes del enfoque anterior, conservadas para notebooks históricos; Gold de control no las usa.
SIN_COMPETENCIA = {
    "Contratación directa",
    "Contratación régimen especial",
    "Solicitud de información a los Proveedores",
    "Subasta de prueba",
}

TERMINADOS = {"Seleccionado", "Cancelado"}

DB = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "secop"),
    "user": os.getenv("POSTGRES_USER", "secop_user"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

def postgres_url() -> str:
    return (
        f"postgresql+psycopg://{DB['user']}:{DB['password']}"
        f"@{DB['host']}:{DB['port']}/{DB['database']}"
    )
