"""Regresiones de credenciales, arranque y ausencia de valores embebidos."""
import ast
from pathlib import Path
from unittest.mock import MagicMock
from urllib.parse import unquote, urlsplit

from dotenv import dotenv_values, set_key
import psycopg2
import pytest

from scripts.configure_env import configure
from src.common.config import airflow_database_url, db_connect

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('reader', [False, True])
def test_database_credentials_come_from_environment(monkeypatch, reader):
    prefix = 'GOLD_READER' if reader else 'POSTGRES'
    monkeypatch.setenv(f'{prefix}_USER', 'usuario_de_prueba')
    monkeypatch.setenv(f'{prefix}_PASSWORD', 'clave_de_prueba')
    connect = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(psycopg2, 'connect', connect)
    with db_connect(reader=reader):
        pass
    assert connect.call_args.kwargs['user'] == 'usuario_de_prueba'
    assert connect.call_args.kwargs['password'] == 'clave_de_prueba'
    connect.return_value.close.assert_called_once()


def test_missing_password_stops_before_connecting(monkeypatch):
    monkeypatch.delenv('POSTGRES_PASSWORD', raising=False)
    connect = MagicMock()
    monkeypatch.setattr(psycopg2, 'connect', connect)
    with pytest.raises(RuntimeError, match='POSTGRES_PASSWORD'):
        with db_connect():
            pass
    connect.assert_not_called()


def test_airflow_url_preserves_special_characters(monkeypatch):
    password = "p@ss:# /'${literal}"
    monkeypatch.setenv('POSTGRES_USER', 'cuenta+prueba')
    monkeypatch.setenv('POSTGRES_PASSWORD', password)
    monkeypatch.setenv('POSTGRES_HOST', 'database.example')
    monkeypatch.setenv('POSTGRES_PORT', '5544')
    url = urlsplit(airflow_database_url())
    assert unquote(url.username) == 'cuenta+prueba'
    assert unquote(url.password) == password
    assert url.hostname == 'database.example'
    assert url.port == 5544


def test_configure_adds_variables_without_rotating_credentials(tmp_path):
    (tmp_path/'.env.example').write_text((ROOT/'.env.example').read_text())
    path = tmp_path/'.env'
    path.touch()
    password = "clave '# $ ${SIN_EXPANDIR} \\ final"
    set_key(path, 'POSTGRES_USER', 'cuenta_existente')
    set_key(path, 'POSTGRES_PASSWORD', password)
    set_key(path, 'GROQ_API_KEY', 'clave_api_ficticia')
    configure(tmp_path)
    first = dotenv_values(path, interpolate=False)
    assert first['POSTGRES_USER'] == 'cuenta_existente'
    assert first['POSTGRES_PASSWORD'] == password
    assert first['GROQ_API_KEY'] == 'clave_api_ficticia'
    assert first['AIRFLOW_DAG_OWNER']
    assert first['AIRFLOW_ADMIN_FIRSTNAME']
    assert first['GOLD_READER_PASSWORD']
    configure(tmp_path)
    assert dotenv_values(path, interpolate=False) == first


def test_runtime_scripts_have_no_literal_credentials():
    sensitive_arguments = {'user', 'username', 'password', 'passwd', 'api_key',
                           'access_key', 'secret_key'}
    violations = []
    for folder in ['src', 'dags', 'scripts']:
        for path in (ROOT/folder).rglob('*.py'):
            for node in ast.walk(ast.parse(path.read_text())):
                if not isinstance(node, ast.Call):
                    continue
                for keyword in node.keywords:
                    if keyword.arg in sensitive_arguments and isinstance(keyword.value, ast.Constant):
                        if isinstance(keyword.value.value, str) and keyword.value.value:
                            violations.append(f'{path.relative_to(ROOT)}:{node.lineno}')
                function = node.func
                if isinstance(function, ast.Attribute) and function.attr == 'getenv' and len(node.args) > 1:
                    key, default = node.args[:2]
                    if isinstance(key, ast.Constant) and isinstance(key.value, str):
                        sensitive = any(token in key.value for token in ['PASSWORD', 'SECRET', 'API_KEY', '_USER'])
                        if sensitive and isinstance(default, ast.Constant) and default.value:
                            violations.append(f'{path.relative_to(ROOT)}:{node.lineno}')
                if isinstance(function, ast.Attribute) and function.attr in ['config', 'set'] and len(node.args) > 1:
                    key, value = node.args[:2]
                    if isinstance(key, ast.Constant) and key.value in ['spark.hadoop.fs.s3a.access.key', 'spark.hadoop.fs.s3a.secret.key', 'fs.s3a.access.key', 'fs.s3a.secret.key']:
                        if isinstance(value, ast.Constant) and isinstance(value.value, str):
                            violations.append(f'{path.relative_to(ROOT)}:{node.lineno}')
    assert not violations, 'Credenciales literales en ' + ', '.join(violations)
