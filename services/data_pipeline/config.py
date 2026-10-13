"""Configuración para futura persistencia. Valida sin conectarse a PostgreSQL.

Soporta .env simple KEY=value, comillas literales y comentarios de línea completa.
No ejecuta interpolación, escapes, export ni comandos. El entorno tiene prioridad.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping
from urllib.parse import parse_qs, urlsplit

REPO_ROOT = Path(__file__).resolve().parents[2]
VARIABLES = ("DATABASE_URL", "PIPELINE_DATABASE_URL", "APP_ENV", "LOG_LEVEL", "DATABASE_SSL_ROOT_CERT")
# Detalles que el panel Connect puede mostrar junto a la URI. No son fuente
# alternativa de conexión: DATABASE_URL sigue siendo la única autoridad.
PANEL_FIELDS = frozenset({"host", "port", "database", "dbname", "user", "password"})


class ConfigurationError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    database_url: str = field(repr=False)
    app_env: str
    log_level: str
    sslmode: str
    sslrootcert: Path | None = field(default=None, repr=False)

    def connection_options(self) -> dict:
        """Opciones psycopg 3 futuras; no registrar este diccionario."""
        options = {"conninfo": self.database_url, "sslmode": self.sslmode,
                   "connect_timeout": 10, "gssencmode": "disable"}
        if self.sslrootcert:
            options["sslrootcert"] = str(self.sslrootcert)
        return options


def _read_env(path: Path) -> dict[str, str]:
    try:
        content = path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError):
        raise ConfigurationError("No se puede leer el archivo de configuración") from None
    values = {}
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if separator and key in PANEL_FIELDS:
            continue
        if not separator or key not in VARIABLES or key in values:
            raise ConfigurationError("Formato .env inválido o variable no soportada")
        if value.startswith(("'", '"')):
            if len(value) < 2 or value[-1] != value[0]:
                raise ConfigurationError("Comillas inválidas en .env")
            value = value[1:-1]
        values[key] = value
    return values


def load_settings(env_file: Path | None = None,
                  environ: Mapping[str, str] | None = None, *, pipeline: bool = False) -> Settings:
    path = Path(env_file) if env_file is not None else REPO_ROOT / ".env"
    values = _read_env(path) if path.exists() else {}
    environment = os.environ if environ is None else environ
    values.update({key: environment[key] for key in VARIABLES if key in environment})
    app_env = values.get("APP_ENV", "development")
    level = values.get("LOG_LEVEL", "INFO")
    if app_env not in {"development", "test", "production"}:
        raise ConfigurationError("APP_ENV debe ser development, test o production")
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigurationError("LOG_LEVEL no soportado")
    key = "PIPELINE_DATABASE_URL" if pipeline else "DATABASE_URL"
    url = values.get(key, "")
    if not url or any(marker in url.upper() for marker in ("CHANGE_ME", "PENDING", "PROJECT_REF", "YOUR_PASSWORD")):
        raise ConfigurationError(f"{key} pendiente de configurar")
    try:
        parsed = urlsplit(url)
        host, port = parsed.hostname, parsed.port
        query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        raise ConfigurationError("DATABASE_URL inválida") from None
    if (parsed.scheme not in {"postgresql", "postgres"} or not host
            or not parsed.username or not parsed.password or parsed.path != "/postgres"
            or parsed.fragment or port != 5432
            or not (host.endswith(".supabase.co") or host.endswith(".pooler.supabase.com"))):
        raise ConfigurationError("DATABASE_URL debe ser URI Supabase directa/session pooler, puerto 5432")
    # Evita query params que redefinan host/usuario/TLS u opciones no revisadas.
    if set(query) - {"sslmode"} or ("sslmode" in query and len(query["sslmode"]) != 1):
        raise ConfigurationError("DATABASE_URL admite un único parámetro sslmode")
    certificate_value = values.get("DATABASE_SSL_ROOT_CERT", "")
    # Una URI copiada sin sslmode no hereda prefer de libpq: exige cifrado.
    # Si hay CA configurada, omitir el modo exige también identidad completa.
    mode = query.get("sslmode", ["verify-full" if certificate_value else "require"])[0]
    if mode not in {"require", "verify-ca", "verify-full"}:
        raise ConfigurationError("TLS obligatorio: sslmode require, verify-ca o verify-full")
    if app_env == "production" and mode != "verify-full":
        raise ConfigurationError("Producción requiere sslmode=verify-full")
    if pipeline and (mode != "verify-full" or parsed.username.split('.')[0] != 'pipeline_runner'):
        raise ConfigurationError("Ingesta requiere pipeline_runner y verify-full")
    certificate = None
    if mode in {"verify-ca", "verify-full"} or certificate_value:
        if not certificate_value:
            raise ConfigurationError("DATABASE_SSL_ROOT_CERT requerido para verificar TLS")
        certificate = Path(certificate_value)
        if not certificate.is_absolute():
            certificate = path.resolve().parent / certificate
        certificate = certificate.resolve()
        if not certificate.is_file():
            raise ConfigurationError("Certificado TLS no disponible")
    return Settings(url, app_env, level, mode, certificate)
