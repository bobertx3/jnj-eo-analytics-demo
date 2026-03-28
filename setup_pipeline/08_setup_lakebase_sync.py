"""
08 — Create Lakebase Provisioned instance and sync Delta tables.

Creates a Lakebase PostgreSQL database and sets up synced tables
from the Bronze/Silver/Gold Delta tables for low-latency app queries.

Usage:
    python setup_pipeline/08_setup_lakebase_sync.py
"""
import os
import sys
import time
import uuid
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────

CATALOG = os.environ.get("CATALOG", "bx4")
SCHEMA = os.environ.get("SCHEMA", "eo_analytics_plane")
INSTANCE_NAME = os.environ.get("LAKEBASE_INSTANCE_NAME", "jnj-eo-analytics-demo")
CAPACITY = "CU_1"  # Smallest tier, sufficient for demo

# Tables to sync with their primary key columns.
# Array columns in Spark will become JSONB in PostgreSQL automatically.
TABLES_TO_SYNC = [
    # Bronze
    {
        "table": "bronze_metrics",
        "pk": ["service_name", "metric_name", "event_timestamp"],
        "policy": "SNAPSHOT",
    },
    {
        "table": "bronze_network_flows",
        "pk": ["src_service", "dst_service", "protocol", "event_timestamp"],
        "policy": "SNAPSHOT",
    },
    # Silver
    {
        "table": "silver_incidents",
        "pk": ["incident_id"],
        "policy": "TRIGGERED",
    },
    {
        "table": "silver_alerts",
        "pk": ["alert_id"],
        "policy": "TRIGGERED",
    },
    {
        "table": "silver_changes",
        "pk": ["change_id"],
        "policy": "TRIGGERED",
    },
    {
        "table": "silver_service_health",
        "pk": ["service_name", "health_date"],
        "policy": "TRIGGERED",
    },
    {
        "table": "silver_servicenow_correlation",
        "pk": ["failure_pattern_id", "business_unit"],
        "policy": "TRIGGERED",
    },
    # Gold
    {
        "table": "gold_root_cause_patterns",
        "pk": ["failure_pattern_id"],
        "policy": "TRIGGERED",
    },
    {
        "table": "gold_service_risk_ranking",
        "pk": ["service_name"],
        "policy": "TRIGGERED",
    },
    {
        "table": "gold_change_incident_correlation",
        "pk": ["change_id", "incident_id"],
        "policy": "TRIGGERED",
    },
    {
        "table": "gold_domain_impact_summary",
        "pk": ["domain", "summary_date"],
        "policy": "TRIGGERED",
    },
    {
        "table": "gold_business_impact_summary",
        "pk": ["business_unit"],
        "policy": "TRIGGERED",
    },
]


def get_workspace_client():
    from databricks.sdk import WorkspaceClient

    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
    if os.environ.get("DATABRICKS_APP_NAME"):
        return WorkspaceClient()
    return WorkspaceClient(profile=profile)


def enable_cdf(w, source_table: str):
    """Enable Change Data Feed on a Delta table (required for TRIGGERED sync)."""
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        warehouses = list(w.warehouses.list())
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                warehouse_id = wh.id
                break
        if not warehouse_id and warehouses:
            warehouse_id = warehouses[0].id

    sql = f"ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    logger.info(f"  Enabling CDF on {source_table}")
    try:
        from databricks.sdk.service.sql import StatementState

        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            logger.warning(f"  CDF enable may have failed: {resp.status.error}")
    except Exception as e:
        logger.warning(f"  Could not enable CDF on {source_table}: {e}")


def create_or_get_instance(w):
    """Create Lakebase instance or return existing one."""
    # Check if instance already exists
    try:
        instance = w.database.get_database_instance(name=INSTANCE_NAME)
        logger.info(f"Lakebase instance '{INSTANCE_NAME}' already exists (state: {instance.state})")
        if instance.state and str(instance.state).upper() == "STOPPED":
            logger.info("Starting stopped instance...")
            w.database.start_database_instance(name=INSTANCE_NAME)
            # Wait for it to start
            for _ in range(60):
                time.sleep(5)
                inst = w.database.get_database_instance(name=INSTANCE_NAME)
                if inst.state and str(inst.state).upper() == "RUNNING":
                    logger.info("Instance is now running.")
                    return inst
            logger.warning("Instance did not start within timeout, proceeding anyway.")
        return instance
    except Exception:
        pass

    # Create new instance
    logger.info(f"Creating Lakebase instance '{INSTANCE_NAME}' with capacity {CAPACITY}...")
    instance = w.database.create_database_instance(
        name=INSTANCE_NAME,
        capacity=CAPACITY,
        stopped=False,
    )
    logger.info(f"Instance created. DNS: {instance.read_write_dns}")

    # Wait for instance to be ready
    logger.info("Waiting for instance to become ready...")
    for i in range(120):
        time.sleep(5)
        inst = w.database.get_database_instance(name=INSTANCE_NAME)
        state = str(inst.state).upper() if inst.state else "UNKNOWN"
        if state == "RUNNING":
            logger.info(f"Instance is ready after ~{(i+1)*5}s")
            return inst
        if i % 12 == 0:
            logger.info(f"  Still waiting... (state: {state})")

    logger.warning("Instance may not be fully ready, proceeding with sync setup.")
    return w.database.get_database_instance(name=INSTANCE_NAME)


def register_catalog(w, instance):
    """Register the instance with Unity Catalog if not already registered."""
    try:
        w.database.register_database_instance(
            name=INSTANCE_NAME,
            catalog=CATALOG,
            schema=SCHEMA,
        )
        logger.info(f"Registered instance with Unity Catalog ({CATALOG}.{SCHEMA})")
    except Exception as e:
        if "already" in str(e).lower() or "exists" in str(e).lower():
            logger.info("Instance already registered with Unity Catalog.")
        else:
            logger.warning(f"Catalog registration warning: {e}")


def setup_synced_tables(w, instance):
    """Create synced tables for all configured Delta tables."""
    from databricks.sdk.service.database import (
        SyncedDatabaseTable,
        SyncedTableSpec,
        SyncedTableSchedulingPolicy,
    )

    policy_map = {
        "SNAPSHOT": SyncedTableSchedulingPolicy.SNAPSHOT,
        "TRIGGERED": SyncedTableSchedulingPolicy.TRIGGERED,
        "CONTINUOUS": SyncedTableSchedulingPolicy.CONTINUOUS,
    }

    for table_config in TABLES_TO_SYNC:
        table_name = table_config["table"]
        source_table = f"{CATALOG}.{SCHEMA}.{table_name}"
        # Target table name in the UC-registered catalog for the Lakebase instance
        target_table = f"{CATALOG}.{SCHEMA}.{table_name}"
        policy = table_config["policy"]
        pk_columns = table_config["pk"]

        # Enable CDF for TRIGGERED/CONTINUOUS sync
        if policy in ("TRIGGERED", "CONTINUOUS"):
            enable_cdf(w, source_table)

        logger.info(f"Creating synced table: {source_table} → Lakebase ({policy})")
        try:
            synced_table = w.database.create_synced_database_table(
                SyncedDatabaseTable(
                    name=target_table,
                    database_instance_name=INSTANCE_NAME,
                    spec=SyncedTableSpec(
                        source_table_full_name=source_table,
                        primary_key_columns=pk_columns,
                        scheduling_policy=policy_map[policy],
                    ),
                )
            )
            logger.info(f"  ✓ Synced table created: {table_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"  ✓ Synced table already exists: {table_name}")
            else:
                logger.error(f"  ✗ Failed to sync {table_name}: {e}")

    logger.info("Synced table setup complete.")


def wait_for_initial_sync(w):
    """Wait for all synced tables to complete their initial sync."""
    logger.info("Waiting for initial sync to complete...")
    pending = set(t["table"] for t in TABLES_TO_SYNC)
    for attempt in range(60):
        still_pending = set()
        for table_name in pending:
            full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
            try:
                status = w.database.get_synced_database_table(name=full_name)
                sync_status = status.data_synchronization_status
                if sync_status:
                    state = str(sync_status.detailed_state).upper() if sync_status.detailed_state else ""
                    if "ACTIVE" in state or "ONLINE" in state or "SUCCEEDED" in state:
                        logger.info(f"  ✓ {table_name} sync complete")
                        continue
                still_pending.add(table_name)
            except Exception:
                still_pending.add(table_name)

        pending = still_pending
        if not pending:
            logger.info("All tables synced successfully!")
            return

        if attempt % 6 == 0:
            logger.info(f"  {len(pending)} tables still syncing: {', '.join(sorted(pending))}")
        time.sleep(10)

    if pending:
        logger.warning(f"Some tables may still be syncing: {', '.join(sorted(pending))}")


def print_summary(instance):
    """Print connection details for .env file."""
    host = instance.read_write_dns if instance.read_write_dns else "<pending>"
    print("\n" + "=" * 60)
    print("Lakebase setup complete!")
    print("=" * 60)
    print(f"\nAdd these to rca_app/.env:\n")
    print(f"LAKEBASE_HOST={host}")
    print(f"LAKEBASE_DATABASE=databricks_postgres")
    print(f"LAKEBASE_INSTANCE_NAME={INSTANCE_NAME}")
    print(f"\n{'=' * 60}\n")


def main():
    logger.info("=" * 60)
    logger.info("Lakebase Sync Setup for EO Analytics")
    logger.info(f"  Catalog: {CATALOG}")
    logger.info(f"  Schema:  {SCHEMA}")
    logger.info(f"  Instance: {INSTANCE_NAME}")
    logger.info(f"  Tables:  {len(TABLES_TO_SYNC)}")
    logger.info("=" * 60)

    w = get_workspace_client()

    # Step 1: Create or get Lakebase instance
    instance = create_or_get_instance(w)

    # Step 2: Register with Unity Catalog
    register_catalog(w, instance)

    # Step 3: Set up synced tables
    setup_synced_tables(w, instance)

    # Step 4: Wait for initial sync
    wait_for_initial_sync(w)

    # Step 5: Print summary
    print_summary(instance)


if __name__ == "__main__":
    main()
