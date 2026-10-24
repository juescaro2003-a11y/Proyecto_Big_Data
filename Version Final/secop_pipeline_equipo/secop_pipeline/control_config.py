"""Reglas operativas iniciales del IRCD, no criterios legales validados."""
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path


@dataclass(frozen=True)
class ControlConfig:
    version: str = 'ircd-1.0'
    cutoff: str = '2026-09-21'
    coverage_start: str = '2017-01-01'
    baseline_start: str = '2022-01-01'
    baseline_end: str = '2024-12-31'
    validation_year: int = 2025
    monitoring_year: int = 2026
    person_filter: str = 'todos'  # todos / natural / indeterminado
    min_entity_contracts: int = 30
    min_reference: int = 30
    min_value_group: int = 30
    p90: float = .90
    p95: float = .95
    high_ratio: float = 1.5
    preliminary_from: int = 2
    priority_from: int = 4
    max_duration_days: int = 1096
    # Solo códigos cuya comparabilidad y remuneración hayan sido revisadas.
    monthly_validated_categories: tuple[str, ...] = ()
    accept_recurrence_ties: bool = False
    value_definition: str = 'valor_total_reportado_snapshot_no_acreditado_como_inicial'
    temporal_mode: str = 'foto_anual_al_corte_no_alerta_en_fecha_firma'

    def __post_init__(self):
        from datetime import date
        start, end, cut, cover = map(date.fromisoformat,
            (self.baseline_start, self.baseline_end, self.cutoff, self.coverage_start))
        if start > end or cover > cut:
            raise ValueError('Intervalos inválidos')
        if self.person_filter not in ('todos', 'natural', 'indeterminado'):
            raise ValueError('Filtro de personas no soportado')
        if not 0 < self.p90 < self.p95 < 1 or self.high_ratio < 1:
            raise ValueError('Percentiles o ratio inválidos')
        if min(self.min_entity_contracts, self.min_reference, self.min_value_group) < 2:
            raise ValueError('Los tamaños mínimos deben ser al menos 2')
        if not 0 < self.preliminary_from < self.priority_from <= 6:
            raise ValueError('Cortes de revisión inválidos')
        if self.max_duration_days < 1:
            raise ValueError('Duración máxima inválida')
        if self.temporal_mode != 'foto_anual_al_corte_no_alerta_en_fecha_firma':
            raise ValueError('Solo se implementa fotografía anual al corte')

    def evidence(self):
        value = asdict(self)
        value['parameter_hash'] = hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
        return value

    @classmethod
    def from_project(cls):
        from .config import OUTPUT_DIR, ROOT
        manifest = json.loads((OUTPUT_DIR / 'manifest_extraccion.json').read_text(encoding='utf-8'))
        path = Path(os.getenv('IRCD_CONFIG') or str(ROOT / 'ircd_config.json'))
        rules = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
        rules.setdefault('cutoff', manifest['cutoff_date'])
        rules['coverage_start'] = manifest['start_date']
        if rules['cutoff'] > manifest['cutoff_date']:
            raise ValueError('El corte analítico supera la cobertura declarada de Bronze')
        if 'monthly_validated_categories' in rules:
            rules['monthly_validated_categories'] = tuple(rules['monthly_validated_categories'])
        return cls(**rules)
