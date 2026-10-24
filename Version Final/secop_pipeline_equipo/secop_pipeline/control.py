"""Preparación auditable de contratos. Las filas originales permanecen en Bronze."""
import hashlib
import json
import re
import unicodedata

import pandas as pd


FIELDS = ['id_contrato', 'nit_entidad', 'codigo_entidad', 'nombre_entidad',
    'documento_proveedor', 'codigo_proveedor', 'tipodocproveedor', 'proveedor_adjudicado',
    'modalidad_de_contratacion', 'justificacion_modalidad_de', 'tipo_de_contrato',
    'codigo_de_categoria_principal', 'departamento', 'descripcion_del_proceso',
    'objeto_del_contrato', 'urlproceso', 'referencia_del_contrato', 'valor_del_contrato',
    'fecha_de_firma', 'fecha_de_inicio_del_contrato', 'fecha_de_fin_del_contrato',
    'ultima_actualizacion']
DATES = ['fecha_de_firma', 'fecha_de_inicio_del_contrato', 'fecha_de_fin_del_contrato', 'ultima_actualizacion']


def normalized(value):
    if pd.isna(value):
        return None
    text = ' '.join(str(value).strip().lower().split())
    text = ''.join(c for c in unicodedata.normalize('NFKD', text) if not unicodedata.combining(c))
    return None if text in ('', 'no definido', 'no definida', 'no informado', 'sin dato', 'n/a', 'null', 'none', '-', '.') else text


def identifier(value):
    """Conserva ceros iniciales y DV: no adivina si el último dígito es verificador."""
    text = normalized(value)
    if text is None:
        return None
    text = re.sub(r'[.\s-]', '', text).upper()
    return text if re.fullmatch(r'[A-Z0-9]+', text) and text not in ('0', '000000000', 'NOAPLICA') else None


def document_type(value):
    text = normalized(value)
    aliases = {'cc': 'CC', 'cedula de ciudadania': 'CC', 'cedula ciudadania': 'CC',
               'ce': 'CE', 'cedula de extranjeria': 'CE', 'cedula de extranjeria colombiana': 'CE',
               'pasaporte': 'PASAPORTE', 'nit': 'NIT'}
    return aliases.get(text)


def normalize_contracts(df):
    out = pd.DataFrame(index=df.index)
    for col in FIELDS:
        raw = df.get('raw__' + col, df.get(col, pd.Series(pd.NA, index=df.index, dtype='string')))
        out['raw__' + col] = raw.astype('string')
        if col in DATES:
            out[col] = pd.to_datetime(df.get(col, raw), errors='coerce', utc=True).dt.tz_convert(None)
        elif col == 'valor_del_contrato':
            out[col] = pd.to_numeric(df.get(col, raw), errors='coerce').astype('float64')
        else:
            out[col] = raw.map(normalized).astype('string')
    out['id_contrato'] = out['raw__id_contrato'].map(lambda v: str(v).strip().upper() if normalized(v) else None).astype('string')
    out['entidad_id'] = out['raw__nit_entidad'].map(identifier).astype('string')
    out['proveedor_documento_normalizado'] = out['raw__documento_proveedor'].map(identifier).astype('string')
    out['tipo_documento_normalizado'] = out['raw__tipodocproveedor'].map(document_type).astype('string')
    # Se prioriza el código estable SECOP; sin él, tipo + documento evita
    # mezclar documentos de distintos sistemas. Nunca se agrupa por nombre.
    typed = out['tipo_documento_normalizado'] + ':' + out['proveedor_documento_normalizado']
    code = out['raw__codigo_proveedor'].map(identifier).astype('string')
    out['proveedor_id'] = ('SECOP:' + code).fillna(typed)
    out['clase_persona'] = pd.Series('indeterminado', index=df.index, dtype='string')
    out.loc[out.tipo_documento_normalizado.isin(['CC', 'CE', 'PASAPORTE']), 'clase_persona'] = 'natural'
    out['causal'] = out['justificacion_modalidad_de']
    # Causal desconocida no se usa como un segmento comparable ficticio.
    out.loc[out.causal.isin(['no aplica', 'no aplica/no pertenece']), 'causal'] = pd.NA
    out['segmento'] = out['causal'] + ' | ' + out['tipo_de_contrato']
    out['fuente_archivo'] = df.get('fuente_archivo', pd.Series('no_registrado', index=df.index)).astype('string')
    raw_cols = ['raw__' + c for c in FIELDS]
    out['huella_registro'] = out[raw_cols].apply(
        lambda row: hashlib.sha256(json.dumps([None if pd.isna(v) else str(v) for v in row], ensure_ascii=False).encode()).hexdigest(), axis=1).astype('string')
    return out


def score_concentration(value, p90, p95):
    if any(pd.isna(v) for v in (value, p90, p95)):
        return None
    return 2 if value >= p95 else 1 if value >= p90 else 0


def score_recurrence(value, p90, p95, accept_ties=False):
    if any(pd.isna(v) for v in (value, p90, p95)) or (p90 == p95 and not accept_ties):
        return None
    return 0 if value < 2 else score_concentration(value, p90, p95)


def score_value(value, median, p90, p95, ratio=1.5):
    if any(pd.isna(v) for v in (value, median, p90, p95)) or median <= 0:
        return None
    return 2 if value >= p95 and value / median >= ratio else 1 if value >= p90 else 0


def compose(c, r, a, preliminary_from=2, priority_from=4):
    if any(pd.isna(v) for v in (c, r, a)):
        return None, 'incompleto', 'componentes parciales; requiere verificar cobertura'
    if any(v not in (0, 1, 2) for v in (c, r, a)):
        raise ValueError('Score fuera de 0, 1, 2')
    total = int(c + r + a)
    return total, 'completo', ('revision prioritaria' if total >= priority_from else
        'revision preliminar' if total >= preliminary_from else 'seguimiento ordinario')
