"""
GreenOps Demo Tools — Simulates a GCP project with idle resources.
Use this to demonstrate the full pipeline in action.
Run: python main_demo.py
"""
import os
from google.adk.tools import FunctionTool


def list_idle_vms(project_id: str) -> dict:
    """[DEMO] Returns simulated idle VMs for demonstration."""
    instances = [
        {
            "name": "ml-training-server-01",
            "zone": "us-central1-a",
            "machineType": "n1-standard-8",
            "status": "RUNNING",
            "lastStartTimestamp": "2026-04-10T09:00:00Z",
            "cpuUtilization": "1.2%",
            "note": "Idle 45 days — ML training job completed"
        },
        {
            "name": "staging-api-backend",
            "zone": "us-central1-b",
            "machineType": "n1-standard-4",
            "status": "RUNNING",
            "lastStartTimestamp": "2026-03-28T14:00:00Z",
            "cpuUtilization": "0.8%",
            "note": "Idle 58 days — staging env not in use"
        },
        {
            "name": "data-pipeline-worker",
            "zone": "us-central1-a",
            "machineType": "n1-standard-2",
            "status": "RUNNING",
            "lastStartTimestamp": "2026-04-20T11:00:00Z",
            "cpuUtilization": "0.3%",
            "note": "Idle 35 days — pipeline migrated to Cloud Run"
        }
    ]
    return {
        "instances": instances,
        "count": len(instances),
        "note": "[DEMO MODE] Simulated idle VMs"
    }


def list_unattached_disks(project_id: str) -> dict:
    """[DEMO] Returns simulated unattached persistent disks."""
    disks = [
        {
            "name": "old-postgres-backup-disk",
            "sizeGb": "500",
            "zone": "us-central1-a",
            "type": "pd-standard",
            "status": "READY",
            "note": "Not attached since Feb 2026 — backup migrated to GCS"
        },
        {
            "name": "dev-workspace-disk",
            "sizeGb": "200",
            "zone": "us-central1-b",
            "type": "pd-ssd",
            "status": "READY",
            "note": "Developer left team — disk orphaned"
        }
    ]
    return {
        "disks": disks,
        "count": len(disks),
        "note": "[DEMO MODE] Simulated unattached disks"
    }


def list_unattached_ips(project_id: str) -> dict:
    """[DEMO] Returns simulated reserved IPs not attached to any resource."""
    ips = [
        {
            "name": "prod-load-balancer-ip-old",
            "address": "34.102.140.239",
            "region": "us-central1",
            "status": "RESERVED",
            "note": "LB decommissioned in March 2026 — IP still reserved"
        }
    ]
    return {
        "ips": ips,
        "count": len(ips),
        "note": "[DEMO MODE] Simulated unused reserved IPs"
    }


def get_recommender_suggestions(project_id: str) -> dict:
    """[DEMO] Returns simulated GCP Recommender rightsizing suggestions."""
    recommendations = [
        {
            "name": "recommender-001",
            "description": "Change machine type of 'analytics-server' from n1-standard-8 to n1-standard-2",
            "primaryImpact": {
                "category": "COST",
                "costProjection": {"cost": {"units": -87}, "duration": "2592000s"}
            },
            "priority": "P2",
            "note": "CPU avg 4% over 30 days — severely over-provisioned"
        }
    ]
    return {
        "recommendations": recommendations,
        "count": len(recommendations),
        "note": "[DEMO MODE] Simulated rightsizing recommendations"
    }


def calculate_carbon(resource_type: str, count: int, hours_per_month: int = 730) -> dict:
    """Calculate estimated CO2 emissions for GCP resources."""
    CARBON_FACTOR = float(os.getenv("CARBON_FACTOR_KWH", "0.000233"))

    POWER_MAP = {
        "idle_vm": 15,
        "unattached_disk": 2,
        "reserved_ip": 0.1,
        "n1-standard-1": 10,
        "n1-standard-2": 20,
        "n1-standard-4": 40,
        "n1-standard-8": 80,
    }

    watts = POWER_MAP.get(resource_type, 10)
    kwh_per_month = (watts * hours_per_month * count) / 1000
    co2_kg = kwh_per_month * CARBON_FACTOR * 1000

    return {
        "resource_type": resource_type,
        "count": count,
        "kwh_per_month": round(kwh_per_month, 2),
        "co2_kg_per_month": round(co2_kg, 4),
        "co2_tons_per_year": round(co2_kg * 12 / 1000, 4)
    }


def stop_vm_instance(project_id: str, instance_name: str, zone: str) -> dict:
    """[DEMO] Simulates stopping a VM instance (no real GCP action taken)."""
    print(f"\n  🛑 [DEMO] Would stop VM: {instance_name} in {zone}")
    return {
        "success": True,
        "instance": instance_name,
        "zone": zone,
        "output": f"[DEMO] VM '{instance_name}' stopped successfully. Estimated saving: $10/month.",
        "error": "",
        "note": "DEMO MODE — no real GCP action taken"
    }


# Register as ADK FunctionTools
list_idle_vms_tool = FunctionTool(list_idle_vms)
list_unattached_disks_tool = FunctionTool(list_unattached_disks)
list_unattached_ips_tool = FunctionTool(list_unattached_ips)
get_recommender_suggestions_tool = FunctionTool(get_recommender_suggestions)
calculate_carbon_tool = FunctionTool(calculate_carbon)
stop_vm_tool = FunctionTool(stop_vm_instance)
