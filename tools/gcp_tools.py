"""
GCP Tools — uses Google Cloud Python SDK (not gcloud CLI).
Works in Cloud Run via Application Default Credentials (ADC).
"""
import os
from google.adk.tools import FunctionTool

# ── Google Cloud Python SDK clients ───────────────────────────────────────────
try:
    from google.cloud import compute_v1
    GCP_COMPUTE_AVAILABLE = True
except ImportError:
    GCP_COMPUTE_AVAILABLE = False

try:
    from google.cloud import recommender_v1
    GCP_RECOMMENDER_AVAILABLE = True
except ImportError:
    GCP_RECOMMENDER_AVAILABLE = False


def list_idle_vms(project_id: str) -> dict:
    """List VM instances that are currently running in the project."""
    if not GCP_COMPUTE_AVAILABLE:
        return {"error": "google-cloud-compute not installed", "instances": []}
    try:
        client = compute_v1.InstancesClient()
        instances = []
        for zone_scope, scope_data in client.aggregated_list(project=project_id):
            if not scope_data.instances:
                continue
            for inst in scope_data.instances:
                if inst.status == "RUNNING":
                    instances.append({
                        "name": inst.name,
                        "zone": zone_scope.split("/")[-1],
                        "machineType": inst.machine_type.split("/")[-1] if inst.machine_type else "unknown",
                        "status": inst.status,
                    })
        return {"instances": instances, "count": len(instances)}
    except Exception as e:
        return {"error": str(e), "instances": [], "count": 0}


def list_unattached_disks(project_id: str) -> dict:
    """Find persistent disks not attached to any VM."""
    if not GCP_COMPUTE_AVAILABLE:
        return {"error": "google-cloud-compute not installed", "disks": []}
    try:
        client = compute_v1.DisksClient()
        disks = []
        for zone_scope, scope_data in client.aggregated_list(project=project_id):
            if not scope_data.disks:
                continue
            for disk in scope_data.disks:
                if not disk.users:
                    disks.append({
                        "name": disk.name,
                        "zone": zone_scope.split("/")[-1],
                        "sizeGb": disk.size_gb,
                        "type": disk.type_.split("/")[-1] if disk.type_ else "unknown",
                        "status": disk.status,
                    })
        return {"disks": disks, "count": len(disks)}
    except Exception as e:
        return {"error": str(e), "disks": [], "count": 0}


def list_unattached_ips(project_id: str) -> dict:
    """Find reserved external IP addresses not in use."""
    if not GCP_COMPUTE_AVAILABLE:
        return {"error": "google-cloud-compute not installed", "ips": []}
    try:
        client = compute_v1.AddressesClient()
        ips = []
        for region_scope, scope_data in client.aggregated_list(project=project_id):
            if not scope_data.addresses:
                continue
            for addr in scope_data.addresses:
                if addr.status == "RESERVED":
                    ips.append({
                        "name": addr.name,
                        "region": region_scope.split("/")[-1],
                        "address": addr.address,
                        "status": addr.status,
                    })
        return {"ips": ips, "count": len(ips)}
    except Exception as e:
        return {"error": str(e), "ips": [], "count": 0}


def get_recommender_suggestions(project_id: str) -> dict:
    """Pull GCP Recommender rightsizing suggestions for VM instances."""
    if not GCP_RECOMMENDER_AVAILABLE:
        return {"error": "google-cloud-recommender not installed", "recommendations": [], "count": 0}
    try:
        client = recommender_v1.RecommenderClient()
        zone = os.getenv("GCP_ZONE", "us-central1-a")
        parent = (
            f"projects/{project_id}/locations/{zone}"
            f"/recommenders/google.compute.instance.MachineTypeRecommender"
        )
        recs = []
        for rec in client.list_recommendations(parent=parent):
            recs.append({
                "name": rec.name,
                "description": rec.description,
                "state": rec.state_info.state.name if rec.state_info else "UNKNOWN",
                "priority": rec.priority.name if rec.priority else "UNKNOWN",
            })
        return {"recommendations": recs, "count": len(recs)}
    except Exception as e:
        return {"error": str(e), "recommendations": [], "count": 0}


def calculate_carbon(resource_type: str, count: int, hours_per_month: int = 730) -> dict:
    """Calculate estimated CO2 emissions for GCP resources.

    Args:
        resource_type: Type of resource (idle_vm, unattached_disk, reserved_ip)
        count: Number of resources
        hours_per_month: Hours per month (default 730 = 24x7)
    """
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
        "co2_tons_per_year": round(co2_kg * 12 / 1000, 4),
    }


def stop_vm_instance(project_id: str, instance_name: str, zone: str) -> dict:
    """Stop a VM instance. ONLY call this after explicit human approval.

    Args:
        project_id: GCP project ID
        instance_name: Name of the VM instance to stop
        zone: Zone where the instance is located
    """
    if not GCP_COMPUTE_AVAILABLE:
        return {"success": False, "error": "google-cloud-compute not installed"}
    try:
        client = compute_v1.InstancesClient()
        operation = client.stop(project=project_id, zone=zone, instance=instance_name)
        op_name = getattr(operation, "name", str(operation))
        return {
            "success": True,
            "instance": instance_name,
            "zone": zone,
            "operation": op_name,
            "output": f"Stop operation initiated for {instance_name} in {zone}",
            "error": "",
        }
    except Exception as e:
        return {
            "success": False,
            "instance": instance_name,
            "zone": zone,
            "output": "",
            "error": str(e),
        }


# ── Register as ADK FunctionTools ─────────────────────────────────────────────
list_idle_vms_tool            = FunctionTool(list_idle_vms)
list_unattached_disks_tool    = FunctionTool(list_unattached_disks)
list_unattached_ips_tool      = FunctionTool(list_unattached_ips)
get_recommender_suggestions_tool = FunctionTool(get_recommender_suggestions)
calculate_carbon_tool         = FunctionTool(calculate_carbon)
stop_vm_tool                  = FunctionTool(stop_vm_instance)
