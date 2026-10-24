"""Calcular el indicador v3 desde Gold. Publicación separada en notebooks 12/13."""
from pathlib import Path
import argparse,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from flows.indicador_v3_flow import indicador_v3_flow

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,default=ROOT/'indicador_v3_config.json')
    p.add_argument('--gold',type=Path,default=ROOT/'data/gold/contratos_control/contratos.parquet')
    a=p.parse_args();result=indicador_v3_flow(config_path=str(a.config),source=str(a.gold))
    print(result['estado'],result['directorio'])
