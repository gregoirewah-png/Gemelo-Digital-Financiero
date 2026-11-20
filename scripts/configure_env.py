"""Completa .env desde la plantilla y conserva las credenciales ya configuradas."""
import base64
from pathlib import Path
import secrets
from urllib.parse import quote

from dotenv import dotenv_values, set_key


def configure(root):
    path = root / '.env'
    template_path = root / '.env.example'
    template = dotenv_values(template_path, interpolate=False)
    if not path.exists():
        path.write_text(template_path.read_text())
    values = dotenv_values(path, interpolate=False)
    for name, value in template.items():
        if not values.get(name):
            values[name] = value or ''
    for name in ['POSTGRES_PASSWORD', 'GOLD_READER_PASSWORD', 'MINIO_ROOT_PASSWORD',
                 'AIRFLOW_ADMIN_PASSWORD', 'AIRFLOW_SECRET_KEY',
                 'PGADMIN_DEFAULT_PASSWORD', 'OPERATIONS_PASSWORD', 'EXECUTIVE_PASSWORD', 'DEMO_ACCESS_CODE']:
        if not values.get(name):
            values[name] = secrets.token_urlsafe(24)
    if not values.get('AIRFLOW_FERNET_KEY'):
        values['AIRFLOW_FERNET_KEY'] = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
    username = quote(values['POSTGRES_USER'], safe='')
    password = quote(values['POSTGRES_PASSWORD'], safe='')
    database = quote(values['POSTGRES_DB'], safe='')
    values['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'] = (
        f"postgresql+psycopg2://{username}:{password}"
        f"@{values['POSTGRES_HOST']}:{values['POSTGRES_PORT']}/{database}"
    )
    values.setdefault('AIRFLOW__CORE__EXECUTOR', 'LocalExecutor')
    for name, value in values.items():
        set_key(path, name, value or '', quote_mode='always')
    path.chmod(0o600)
    print('Configuración guardada en .env. Las credenciales existentes se conservaron.')


if __name__ == '__main__':
    configure(Path(__file__).resolve().parents[1])
