"""Publicación explícita de resultados ya calculados; no inicia Prefect ni el ETL."""
from pathlib import Path
import argparse,json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from secop_pipeline.alertas_aws import check_connection
from secop_pipeline.alertas_publicacion import publish_completed_result

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--resultados',type=Path)
    parser.add_argument('--solo-conexion',action='store_true')
    args=parser.parse_args()
    print(check_connection(),flush=True)
    if not args.solo_conexion:
        result=args.resultados
        if result is None:
            from secop_pipeline.indicador_v3 import latest_result
            result=latest_result(ROOT)
        publication=publish_completed_result(result)
        print(json.dumps(publication['publicacion'],ensure_ascii=False),flush=True)
        for name,value in publication['verificacion'].items():
            print(name,flush=True)
            print(value.to_string(index=False) if hasattr(value,'to_string') else value,flush=True)
