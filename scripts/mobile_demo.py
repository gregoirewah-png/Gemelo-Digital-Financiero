"""Publica solo la demo de usuario y conserva activas las protecciones de Streamlit.
Uso: python3 scripts/mobile_demo.py start|status|stop
"""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / '.mobile-demo'
URL_PATTERN = r'https://[a-z0-9-]+\.trycloudflare\.com'


def compose(*args, capture=False, extra_env=None):
    return subprocess.run(['docker', 'compose', '--profile', 'mobile', *args], cwd=ROOT,
                          env={**os.environ, **(extra_env or {})}, check=True,
                          text=True, capture_output=capture)


def keep_awake():
    if sys.platform != 'darwin':
        return
    pidfile = STATE / 'awake.pid'
    if pidfile.exists():
        check = subprocess.run(['ps', '-p', pidfile.read_text().strip(), '-o', 'command='],
                               capture_output=True, text=True)
        if check.stdout.strip() == 'caffeinate -i -s':
            return
    process = subprocess.Popen(['caffeinate', '-i', '-s'], stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               start_new_session=True)
    pidfile.write_text(str(process.pid))


def stop_awake():
    pidfile = STATE / 'awake.pid'
    if pidfile.exists():
        value = pidfile.read_text().strip()
        check = subprocess.run(['ps', '-p', value, '-o', 'command='], capture_output=True, text=True)
        if value.isdigit() and check.stdout.strip() == 'caffeinate -i -s':
            os.kill(int(value), signal.SIGTERM)
        pidfile.unlink(missing_ok=True)


def start():
    STATE.mkdir(mode=0o700, exist_ok=True)
    STATE.chmod(0o700)
    compose('up', '-d', 'mobile_user', 'mobile_tunnel')
    deadline = time.monotonic() + 120
    url = None
    while time.monotonic() < deadline:
        logs = compose('logs', '--no-color', 'mobile_tunnel', capture=True).stdout
        matches = re.findall(URL_PATTERN, logs)
        if matches:
            url = matches[-1]
            break
        time.sleep(2)
    if not url:
        raise RuntimeError('Cloudflare no entregó una dirección. Revisa docker compose logs mobile_tunnel.')
    # Mantiene el mismo túnel; solo adapta el origen permitido por Streamlit.
    host = urlsplit(url).hostname
    compose('up', '-d', '--no-deps', 'mobile_user', extra_env={'PUBLIC_USER_HOST': host})
    for _ in range(45):
        rows = compose('ps', '--format', 'json', 'mobile_user', capture=True).stdout.strip()
        if rows:
            row = json.loads(rows)
            if isinstance(row, list):
                row = row[0] if row else {}
            if row.get('Health') == 'healthy':
                break
        time.sleep(2)
    else:
        raise RuntimeError('La vista móvil no está saludable; el túnel sigue disponible para diagnóstico.')
    (STATE / 'url.txt').write_text(url + '\n')
    keep_awake()
    print('\nAbre en Safari: ' + url)
    print('La clave está en DEMO_ACCESS_CODE de .env. No se incluye en el enlace.')
    print('Mantén la Mac conectada a corriente, con la tapa abierta e internet. La pantalla puede apagarse.')
    print('El enlace es temporal: vuelve a ejecutar start y consulta status antes de presentar.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'status', 'stop'])
    args = parser.parse_args()
    if args.action == 'start':
        start()
    elif args.action == 'status':
        compose('ps', 'mobile_user', 'mobile_tunnel')
        path = STATE / 'url.txt'
        print('Último enlace guardado:', path.read_text().strip() if path.exists() else 'sin iniciar')
        print('Comprueba que el túnel siga activo: un reinicio puede cambiar la dirección.')
    else:
        compose('stop', 'mobile_tunnel', 'mobile_user')
        stop_awake()
        (STATE / 'url.txt').unlink(missing_ok=True)
        print('Acceso externo detenido. La Mac vuelve a su política normal de reposo.')


if __name__ == '__main__':
    main()
