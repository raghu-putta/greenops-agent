import subprocess
import json
import os
import shutil
from google.adk.tools import FunctionTool


def _gcloud(*args) -> subprocess.CompletedProcess:
    """Run a gcloud command, finding the correct executable on Windows."""
    gcloud_cmd = shutil.which("gcloud") or shutil.which("gcloud.cmd")
    if gcloud_cmd is None:
        # Fallback: common install paths on Windows
        candidates = [
            os.path.expanduser(r"~\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"),
            r"C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
            r"C:\Program Files\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
        ]
        for c in candidates:
            if os.path.exists(c):
                gcloud_cmd = c
                break
    if gcloud_cmd is None:
        raise FileNotFoundError("gcloud not found. Run: gcloud auth application-default login")
    return subprocess.run([gcloud_cmd, *args], capture_output=True, text=True, shell=False)


def list_idle_vms(project_id: str) -> dict:
    """List VM instances that are currently running in the project."""
    result = _gcloud(
        "compute", "instances", "list",
        "--project", project_id,
        "--format", "json",
        "--filter", "status=RUNNING"
    )

    if result.returncode != 0:
        return {"error": result.stderr, "instances": []}

    try:
        instances = json.loads(result.stdout) if result.stdout.strip() else []
    except json.JSONDecodeError:
        instances = []

    return {"instances": instances, "count": len(instances)}


def list_unattached_disks(project_id: str) -> dict:
    """Find persistent disks not attached to any VM."""
    result = _gcloud(
        "compute", "disks", "list",
        "--project", project_id,
        "--format", "json",
        "--filter", "NOT users:*"
    )

    try:
        disks = json.loads(result.stdout) if result.stdout.strip() else []
    except json.JSONDecodeError:
        disks = []

    return {"disks": disks, "count": len(disks)}


def list_unattached_ips(project_id: str) -> dict:
    """Find reserved external IP addresses not attached to any resource."""
    result = _gcloud(
        "compute", "addresses", "list",
        "--project", project_id,
        "--format", "json",
        "--filter", "status=RESERVED"
    )

    try:
        ips = json.loads(result.stdout) if result.stdout.strip() else []
    except json.JSONDecodeError:
        ips = []

    return {"ips": ips, "count": len(ips)}


def get_recommender_suggestions(project_id: str) -> dict:
    """Pull GCP Recommender rightsizing suggestions for VM instances."""
    result = _gcloud(
        "recommender", "recommendations", "list",
        "--project", project_id,
        "--recommender", "google.compute.instance.MachineTypeRecommender",
        "--location", "us-central1-a",
        "--format", "json"
    )

    try:
        recs = json.loads(result.stdout) if result.stdout.strip() else []
    except json.JSONDecodeError:
        recs = []

    return {"recommendations": recs, "count": len(recs)}


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
        "co2_tons_per_year": round(co2_kg * 12 / 1000, 4)
    }


def stop_vm_instance(project_id: str, instance_name: str, zone: str) -> dict:
    """Stop a VM instance. ONLY call this after explicit human approval.

    Args:
        project_id: GCP project ID
        instance_name: Name of the VM instance to stop
        zone: Zone where the instance is located
    """
    result = _gcloud(
        "compute", "instances", "stop",
        instance_name,
        "--zone", zone,
        "--project", project_id,
        "--quiet"
    )

    return {
        "success": result.returncode == 0,
        "instance": instance_name,
        "zone": zone,
        "output": result.stdout,
        "error": result.stderr
    }


# Register as ADK FunctionTools
list_idle_vms_tool = FunctionTool(list_idle_vms)
list_unattached_disks_tool = FunctionTool(list_unattached_disks)
list_unattached_ips_tool = FunctionTool(list_unattached_ips)
get_recommender_suggestions_tool = FunctionTool(get_recommender_suggestions)
calculate_carbon_tool = FunctionTool(calculate_carbon)
stop_vm_tool = FunctionTool(stop_vm_instance)
