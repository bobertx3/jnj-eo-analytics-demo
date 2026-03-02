"""
Database connection module for Databricks SQL Warehouse.
Supports both local development (with profile) and deployed App (with service principal).
"""
import os
import time
import logging
from typing import Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logger = logging.getLogger(__name__)

CATALOG = os.environ.get("CATALOG", "bx4")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics_plane")

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))


def get_workspace_client() -> WorkspaceClient:
    """Get WorkspaceClient with proper auth for environment."""
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
    return WorkspaceClient(profile=profile)


def get_workspace_host() -> str:
    """Get workspace host URL with https:// prefix."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    w = get_workspace_client()
    return w.config.host


def get_oauth_token() -> Optional[str]:
    """Get OAuth token for API calls."""
    w = get_workspace_client()
    if w.config.token:
        return w.config.token
    auth_headers = w.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    return None


_warehouse_id_cache = None

def get_warehouse_id() -> str:
    """Find a running SQL warehouse."""
    global _warehouse_id_cache
    if _warehouse_id_cache:
        return _warehouse_id_cache

    # Check env first (from app resource)
    env_wh = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if env_wh:
        _warehouse_id_cache = env_wh
        return env_wh

    w = get_workspace_client()
    warehouses = list(w.warehouses.list())
    # Prefer serverless, then running
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING" and wh.enable_serverless_compute:
            _warehouse_id_cache = wh.id
            return wh.id
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            _warehouse_id_cache = wh.id
            return wh.id
    if warehouses:
        _warehouse_id_cache = warehouses[0].id
        return warehouses[0].id
    raise RuntimeError("No SQL warehouse found")


def execute_query(sql: str, params: dict = None) -> list[dict]:
    """Execute SQL and return results as list of dicts."""
    w = get_workspace_client()
    warehouse_id = get_warehouse_id()

    # Parameter substitution (simple)
    if params:
        for key, value in params.items():
            if isinstance(value, str):
                sql = sql.replace(f":{key}", f"'{value}'")
            else:
                sql = sql.replace(f":{key}", str(value))

    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="50s",
            catalog=CATALOG,
            schema=SCHEMA,
        )

        if resp.status and resp.status.state == StatementState.SUCCEEDED:
            return _parse_result(resp)
        elif resp.status and resp.status.state == StatementState.FAILED:
            logger.error(f"Query failed: {resp.status.error}")
            raise RuntimeError(f"Query failed: {resp.status.error}")
        else:
            # Poll for completion
            stmt_id = resp.statement_id
            for _ in range(30):
                time.sleep(2)
                status = w.statement_execution.get_statement(stmt_id)
                if status.status.state == StatementState.SUCCEEDED:
                    return _parse_result(status)
                if status.status.state == StatementState.FAILED:
                    raise RuntimeError(f"Query failed: {status.status.error}")
            raise RuntimeError("Query timed out after 60s")
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise


def _parse_result(resp) -> list[dict]:
    """Parse SQL statement response into list of dicts."""
    if not resp.result or not resp.result.data_array:
        return []

    columns = []
    if resp.manifest and resp.manifest.schema and resp.manifest.schema.columns:
        columns = [col.name for col in resp.manifest.schema.columns]
    else:
        return [{"col_" + str(i): v for i, v in enumerate(row)} for row in resp.result.data_array]

    results = []
    for row in resp.result.data_array:
        record = {}
        for i, col_name in enumerate(columns):
            record[col_name] = row[i] if i < len(row) else None
        results.append(record)
    return results
