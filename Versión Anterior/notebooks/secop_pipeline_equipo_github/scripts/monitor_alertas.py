"""python scripts/monitor_alertas.py --watch --open-dashboard"""
import argparse
from pathlib import Path
import sys
import time
import webbrowser

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from secop_pipeline.alertas_monitor import snapshot,text_status


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--watch',action='store_true')
    p.add_argument('--open-dashboard',action='store_true')
    a=p.parse_args();opened=set()
    try:
        while True:
            try:
                s=snapshot();print(text_status(s),flush=True)
                url=s.get('dashboard_url')
                if a.open_dashboard and url and url not in opened:
                    from urllib.parse import urlparse
                    if urlparse(url).hostname in ('127.0.0.1','localhost'):
                        webbrowser.open(url);opened.add(url)
                if not a.watch or s['estado'] in ('COMPLETADA','FALLIDA'):break
            except (OSError,ValueError):
                print('Registro en actualización; reintentando...',flush=True)
                if not a.watch:break
            time.sleep(5)
    except KeyboardInterrupt:print('Monitor cerrado; el flujo continúa.')


if __name__=='__main__':main()
