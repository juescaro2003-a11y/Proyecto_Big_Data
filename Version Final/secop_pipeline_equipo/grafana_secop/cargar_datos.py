"""CLI de publicación; los cálculos analíticos permanecen en el pipeline."""
from pathlib import Path
import argparse
import json
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import psycopg
from grafana_secop.publicacion import run
from grafana_secop.runtime import config,connection_args,inspect_database
from secop_pipeline.indicador_v3 import latest_result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--resultados',type=Path)
    parser.add_argument('--inspect',action='store_true')
    args=parser.parse_args();cfg=config(args.root)
    if args.inspect:
        print(inspect_database(args.root,cfg));return
    result=args.resultados or latest_result(args.root)
    report=json.loads(result.with_name('ejecucion.json').read_text(encoding='utf-8'))
    publisher=run
    if report.get('version')=='alertas-cd-3.0':
        from grafana_secop.publicacion_v3 import run as publisher
    answer=publisher(args.root,result,cfg,
               lambda:psycopg.connect(**connection_args(args.root,cfg)))
    print(answer)


if __name__=='__main__':main()
