"""
04_create_genie_space.py
Creates a Databricks Genie Space for natural language Q&A
over the Enterprise RCA Intelligence gold/silver tables.

Note: Tables must be added to the Genie Space manually via the UI at:
  https://<workspace>/explore/genie/<space_id>

Tables to add:
  - jnj_eo_demo.eo_analytics_plane.gold_root_cause_patterns
  - jnj_eo_demo.eo_analytics_plane.gold_service_risk_ranking
  - jnj_eo_demo.eo_analytics_plane.gold_business_impact_summary
  - jnj_eo_demo.eo_analytics_plane.gold_domain_impact_summary
  - jnj_eo_demo.eo_analytics_plane.silver_incidents
  - jnj_eo_demo.eo_analytics_plane.silver_servicenow_correlation
"""
import os
import json
from databricks.sdk import WorkspaceClient

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
WAREHOUSE_ID = "08381690ac2b0e1a"

SPACE_TITLE = "Enterprise RCA Intelligence -- Business Impact Q&A"
SPACE_DESCRIPTION = (
    "Ask natural language questions about incidents, root causes, and business impact "
    "across JnJ business domains including Supply Chain, Digital Surgery, "
    "Clinical Trials, and Commercial Pharma."
)


def main():
    w = WorkspaceClient(profile=PROFILE)

    print(f"Creating Genie Space ...")
    print(f"  Warehouse: {WAREHOUSE_ID}")

    space = w.genie.create_space(
        warehouse_id=WAREHOUSE_ID,
        serialized_space=json.dumps({"version": 2}),
        title=SPACE_TITLE,
        description=SPACE_DESCRIPTION,
    )

    space_id = space.space_id
    host = w.config.host

    print(f"\n  Genie Space created successfully!")
    print(f"  Space ID: {space_id}")
    print(f"  URL: {host}/explore/genie/{space_id}")
    print(f"\n  GENIE_SPACE_ID={space_id}")
    print("\n  IMPORTANT: Add tables to this Genie Space via the UI:")
    print("    - gold_root_cause_patterns")
    print("    - gold_service_risk_ranking")
    print("    - gold_business_impact_summary")
    print("    - gold_domain_impact_summary")
    print("    - silver_incidents")
    print("    - silver_servicenow_correlation")

    return space_id


if __name__ == "__main__":
    main()
