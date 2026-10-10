"""Ejecutar desde la raíz: python scripts/run_alertas.py --local."""
import argparse
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))


def main():
    p=argparse.ArgumentParser(description='Analítica posterior a Gold; no reconstruye Bronze, Silver o Gold')
    modes=p.add_mutually_exclusive_group()
    modes.add_argument('--local',action='store_true',help='Calcular sin AWS (predeterminado)')
    modes.add_argument('--aws',action='store_true',help='Calcular/reutilizar y publicar en AWS')
    modes.add_argument('--solo-carga',action='store_true',help='Publicar resultado validado existente, sin recalcular')
    modes.add_argument('--check-aws',action='store_true',help='Solo SELECT 1 y comprobación SSL')
    p.add_argument('--config',default=str(ROOT/'alertas_config.json'))
    p.add_argument('--cutoff',help='Corte YYYY-MM-DD, como máximo el corte de Gold')
    p.add_argument('--escenarios',action='store_true',help='Evaluar 27 combinaciones sin recalcular referencias')
    a=p.parse_args()
    if a.check_aws:
        from secop_pipeline.alertas_aws import check_connection
        print(check_connection());return
    from flows.alertas_flow import alertas_flow
    alertas_flow(config_path=a.config,cutoff=a.cutoff,publish_aws=a.aws or a.solo_carga,
                 only_load=a.solo_carga,scenarios=a.escenarios)


if __name__=='__main__': main()
