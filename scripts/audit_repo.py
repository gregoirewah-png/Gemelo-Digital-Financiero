"""Comprueba lo que se enviaría a Git sin leer ni imprimir secretos del .env."""
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
RUNTIME=('postgres_data/','logs/','models/','.venv/','.backups/','.mobile-demo/',
         'data/bronze/','data/silver/','data/gold/','data/quarantine/','data/metadata/')


def forbidden(path):
    parts=Path(path).parts
    name=Path(path).name
    return (path.startswith(RUNTIME) or '__pycache__' in parts or '.pytest_cache' in parts
            or name=='.DS_Store' or name.endswith(('.pyc','.pyo'))
            or (name.startswith('.env') and name!='.env.example'))


def audit(root=ROOT):
    result=subprocess.run(['git','ls-files','-z'],cwd=root,check=True,capture_output=True)
    tracked=[p for p in result.stdout.decode().split('\0') if p]
    issues=['Archivo de ejecución o secreto versionado: '+p for p in tracked if forbidden(p)]
    for path in tracked:
        file=root/path
        if file.is_file() and file.stat().st_size>25*1024*1024:
            issues.append('Archivo mayor de 25 MiB; revisar distribución: '+path)
    for line in (root/'.env.example').read_text().splitlines():
        if not line.strip() or line.lstrip().startswith('#') or '=' not in line:continue
        name,value=line.split('=',1)
        if any(key in name for key in ('PASSWORD','SECRET','API_KEY','FERNET_KEY','ACCESS_CODE')):
            if value.strip().strip('\"\''):
                issues.append('La plantilla debe dejar vacío el secreto '+name.strip())
    return tracked,issues


if __name__=='__main__':
    files,issues=audit()
    for issue in issues:print(issue)
    print(f'{len(files)} archivos versionados; {len(issues)} observaciones.')
    sys.exit(bool(issues))
