"""Conexión y catálogo de solo lectura. No contiene DDL/DML ni carga de datos."""
from __future__ import annotations

from .config import Settings

PROJECT_TABLES = (
    "data_sources", "actors", "raw_events", "normalized_events", "feature_windows",
    "model_versions", "detection_results", "alerts", "alert_reviews", "ingest_batches",
)


class DatabaseCheckError(Exception):
    """Categoría segura; nunca incluye mensaje original del controlador."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _error_code(error) -> str:
    state = getattr(error, "sqlstate", None)
    if state in {"28P01", "28000"}:
        return "DB_AUTHENTICATION_FAILED"
    if state == "42501":
        return "DB_PERMISSION_DENIED"
    if state == "57014":
        return "DB_QUERY_TIMEOUT"
    # Se inspecciona en memoria, nunca se devuelve/loguea el texto original.
    message = str(error).casefold()
    if "10013" in message or "not permitted by its access permissions" in message:
        return "DB_NETWORK_RESTRICTED"
    if any(term in message for term in ("certificate", "ssl error", "tls", "root certificate")):
        return "DB_TLS_FAILED_NO_DOWNGRADE"
    if any(term in message for term in ("translate host", "name resolution", "getaddrinfo", "nodename")):
        return "DB_DNS_FAILED"
    if any(term in message for term in ("timeout", "timed out")):
        return "DB_CONNECTION_TIMEOUT"
    if any(term in message for term in ("password authentication", "authentication failed")):
        return "DB_AUTHENTICATION_FAILED"
    return "DB_CONNECTION_OR_QUERY_FAILED"


def check_database(settings: Settings, audit: bool = False) -> dict:
    """Exige TLS y READ ONLY antes de primer SELECT; rollback y cierre final."""
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:
        raise DatabaseCheckError("DB_DRIVER_NOT_INSTALLED_IN_THIS_ENVIRONMENT") from None
    connection = None
    try:
        options = settings.connection_options()
        # No aceptar prefer/allow implícitos. Opciones separadas dominan la URI.
        options["options"] = "-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=3000"
        connection = psycopg.connect(**options, row_factory=dict_row, prepare_threshold=None)
        connection.read_only = True
        if not connection.pgconn.ssl_in_use:
            raise DatabaseCheckError("DB_TLS_NOT_ACTIVE")
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 AS select_one, current_setting('server_version') AS server_version, "
                           "current_database() AS database, current_setting('transaction_read_only') AS read_only, "
                           "current_setting('default_transaction_read_only') AS default_read_only")
            result = cursor.fetchone()
            # El session pooler puede ignorar defaults de startup. read_only=True
            # genera BEGIN READ ONLY; validar la transacción efectiva es obligatorio.
            if result["select_one"] != 1 or result["read_only"] != "on":
                raise DatabaseCheckError("DB_READ_ONLY_NOT_CONFIRMED")
            cursor.execute("SELECT version()")
            cursor.fetchone()  # No imprimir SO/compilador/otros detalles innecesarios.
            cursor.execute("SELECT ssl, version, cipher, bits FROM pg_catalog.pg_stat_ssl WHERE pid=pg_backend_pid()")
            server_ssl = cursor.fetchone()
            output = {"connection": "ok", "select_one": 1, "database": result["database"],
                      "postgresql_version": result["server_version"], "transaction_read_only": True,
                      "session_default_read_only": result["default_read_only"] == "on",
                      "tls": {"client_to_endpoint": True, "sslmode": settings.sslmode,
                              "certificate_identity_verified": settings.sslmode == "verify-full",
                              "server_pg_stat_ssl": server_ssl},
                      "writes_executed": False}
            if audit:
                output["audit"] = _audit_schema(cursor)
            return output
    except DatabaseCheckError:
        raise
    except psycopg.Error as error:
        raise DatabaseCheckError(_error_code(error)) from None
    finally:
        if connection is not None:
            try:
                connection.rollback()
            except psycopg.Error:
                pass  # El error remoto original nunca se expone durante cleanup.
            finally:
                try:
                    connection.close()
                except psycopg.Error:
                    pass


def _rows(cursor, statement: str, parameters=None):
    cursor.execute(statement, parameters)
    return cursor.fetchall()


def _audit_schema(cursor) -> dict:
    """Solo catálogos, sin contenidos de tablas, DSN, contraseñas ni roles login."""
    schemas = _rows(cursor, "SELECT nspname AS schema FROM pg_catalog.pg_namespace "
                    "WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema' ORDER BY nspname")
    tables = _rows(cursor, "SELECT n.nspname AS schema, c.relname AS name, c.relkind AS kind, "
                   "c.relrowsecurity AS rls_enabled, c.relforcerowsecurity AS rls_forced, "
                   "pg_catalog.pg_get_userbyid(c.relowner)=current_user AS session_is_owner "
                   "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                   "WHERE c.relname=ANY(%s) ORDER BY n.nspname,c.relname",
                   (list(PROJECT_TABLES),))
    columns = _rows(cursor, "SELECT n.nspname AS schema, c.relname AS table_name, a.attname AS column_name, "
                    "pg_catalog.format_type(a.atttypid,a.atttypmod) AS type, a.attnotnull AS not_null, "
                    "a.atthasdef AS has_default FROM pg_catalog.pg_attribute a "
                    "JOIN pg_catalog.pg_class c ON c.oid=a.attrelid "
                    "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE c.relname=ANY(%s) AND a.attnum>0 AND NOT a.attisdropped "
                    "ORDER BY n.nspname,c.relname,a.attnum", (list(PROJECT_TABLES),))
    constraints = _rows(cursor, "SELECT n.nspname AS schema, c.relname AS table_name, co.conname AS name, "
                        "co.contype AS type, co.convalidated AS validated, "
                        "ARRAY(SELECT a.attname FROM unnest(co.conkey) WITH ORDINALITY k(num,ord) "
                        "JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.num ORDER BY k.ord) AS columns, "
                        "rn.nspname AS referenced_schema, rc.relname AS referenced_table "
                        "FROM pg_catalog.pg_constraint co JOIN pg_catalog.pg_class c ON c.oid=co.conrelid "
                        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                        "LEFT JOIN pg_catalog.pg_class rc ON rc.oid=co.confrelid "
                        "LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid=rc.relnamespace "
                        "WHERE c.relname=ANY(%s) ORDER BY n.nspname,c.relname,co.conname", (list(PROJECT_TABLES),))
    grants = _rows(cursor, "SELECT n.nspname AS schema, c.relname AS table_name, r.rolname AS api_role, "
                   "has_table_privilege(r.oid,c.oid,'SELECT') AS can_select, "
                   "has_table_privilege(r.oid,c.oid,'INSERT') AS can_insert, "
                   "has_table_privilege(r.oid,c.oid,'UPDATE') AS can_update, "
                   "has_table_privilege(r.oid,c.oid,'DELETE') AS can_delete "
                   "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                   "CROSS JOIN pg_catalog.pg_roles r WHERE c.relname=ANY(%s) "
                   "AND c.relkind IN ('r','p','v','m','f') AND r.rolname IN ('anon','authenticated','service_role') "
                   "ORDER BY n.nspname,c.relname,r.rolname", (list(PROJECT_TABLES),))
    policies = _rows(cursor, "SELECT schemaname AS schema, tablename AS table_name, policyname AS name, "
                     "roles, cmd, permissive, qual IN ('true','(true)') AS using_all_rows, "
                     "with_check IN ('true','(true)') AS check_all_rows "
                     "FROM pg_catalog.pg_policies WHERE tablename=ANY(%s) ORDER BY schemaname,tablename,policyname",
                     (list(PROJECT_TABLES),))
    session = _rows(cursor, "SELECT rolsuper AS superuser, rolbypassrls AS bypass_rls, "
                    "(SELECT datdba=r.oid FROM pg_catalog.pg_database WHERE datname=current_database()) AS database_owner "
                    "FROM pg_catalog.pg_roles r WHERE rolname=current_user")
    public_counts = _rows(cursor, "SELECT count(*) AS table_count FROM pg_catalog.pg_class c "
                          "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                          "WHERE n.nspname='public' AND c.relkind IN ('r','p')")
    extensions = _rows(cursor, "SELECT e.extname AS name, e.extversion AS version, n.nspname AS schema "
                       "FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace "
                       "WHERE e.extname IN ('pgcrypto','uuid-ossp') ORDER BY e.extname")
    schema_grants = _rows(cursor, "SELECT n.nspname AS schema, r.rolname AS api_role, "
                          "has_schema_privilege(r.oid,n.oid,'USAGE') AS can_use, "
                          "has_schema_privilege(r.oid,n.oid,'CREATE') AS can_create "
                          "FROM pg_catalog.pg_namespace n CROSS JOIN pg_catalog.pg_roles r "
                          "WHERE n.nspname='public' AND r.rolname IN ('anon','authenticated','service_role') ORDER BY r.rolname")
    defaults = _rows(cursor, "SELECT n.nspname AS schema, d.defaclobjtype AS object_type, "
                    "d.defaclrole=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user) AS creator_is_session, "
                    "CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE r.rolname END AS api_role, a.privilege_type "
                    "FROM pg_catalog.pg_default_acl d "
                    "CROSS JOIN LATERAL aclexplode(d.defaclacl) a "
                    "LEFT JOIN pg_catalog.pg_namespace n ON n.oid=d.defaclnamespace "
                    "LEFT JOIN pg_catalog.pg_roles r ON r.oid=a.grantee "
                    "WHERE (n.nspname='public' OR d.defaclnamespace=0) "
                    "AND (a.grantee=0 OR r.rolname IN ('anon','authenticated','service_role')) "
                    "ORDER BY n.nspname,d.defaclobjtype,api_role,a.privilege_type")
    return {"schemas": [r["schema"] for r in schemas], "project_objects": tables,
            "columns": columns, "constraints": constraints, "api_grants": grants,
            "policies": policies, "session_role_capabilities": session,
            "public_table_count": public_counts[0]["table_count"],
            "uuid_extensions": extensions, "public_schema_api_grants": schema_grants,
            "default_api_grants": defaults,
            "missing_initial_public_tables": [name for name in PROJECT_TABLES[:-1]
                                               if not any(t["schema"] == "public" and t["name"] == name for t in tables)],
            "data_api_exposed_schemas": "not_confirmed_from_dashboard"}
