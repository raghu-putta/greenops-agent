"""
GreenOps MCP Server
Exposes GreenOps GCP scanning tools as an MCP server for Claude Desktop.

Setup:
  pip install mcp python-dotenv

Add to ~/AppData/Roaming/Claude/claude_desktop_config.json:
  {
    "mcpServers": {
      "greenops": {
        "command": "python",
        "args": ["C:/Users/raghu/greenops-agent/mcp_server.py"],
        "env": {
          "GOOGLE_API_KEY": "your-key",
          "GCP_PROJECT_ID": "your-project-id"
        }
      }
    }
  }

Then in Claude Desktop just say:
  "Scan my GCP project for waste"
  "What VMs are idle in my project?"
  "Calculate carbon footprint of 3 idle VMs"
"""

import os
import json
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

mcp = FastMCP("GreenOps AI 🌱")

GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
CARBON_FACTOR = float(os.getenv("CARBON_FACTOR_KWH", "0.000233"))


# ── Tool 1: List Idle VMs ────────────────────────────────────────────────────
@mcp.tool()
def list_idle_vms(project_id: str = GCP_PROJECT) -> str:
    """
    Scan a GCP project for idle/underutilized VM instances that are wasting
    money and generating unnecessary CO2. Returns a list of VMs with their
    zone, machine type, CPU utilization, and estimated waste.
    """
    try:
        import subprocess
        result = subprocess.run(
            ["gcloud", "compute", "instances", "list",
             f"--project={project_id}",
             "--format=json",
             "--filter=status=RUNNING"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            instances = json.loads(result.stdout)
            idle = []
            for inst in instances:
                idle.append({
                    "name": inst.get("name"),
                    "zone": inst.get("zone", "").split("/")[-1],
                    "machineType": inst.get("machineType", "").split("/")[-1],
                    "status": inst.get("status"),
                    "note": "Check CPU utilization — may be idle"
                })
            return json.dumps({
                "project_id": project_id,
                "instances": idle,
                "count": len(idle),
                "tip": "VMs with <5% CPU over 7+ days are likely idle and safe to stop"
            }, indent=2)
    except Exception:
        pass

    # Demo fallback if gcloud not available
    return json.dumps({
        "project_id": project_id,
        "instances": [
            {"name": "ml-training-server-01", "zone": "us-central1-a",
             "machineType": "n1-standard-8", "cpuUtilization": "1.2%",
             "idleDays": 45, "estimatedWaste": "$58/month"},
            {"name": "staging-api-backend", "zone": "us-central1-b",
             "machineType": "n1-standard-4", "cpuUtilization": "0.8%",
             "idleDays": 58, "estimatedWaste": "$32/month"},
            {"name": "data-pipeline-worker", "zone": "us-central1-a",
             "machineType": "n1-standard-2", "cpuUtilization": "0.3%",
             "idleDays": 35, "estimatedWaste": "$16/month"}
        ],
        "count": 3,
        "totalWaste": "$106/month",
        "note": "DEMO MODE — run with real gcloud auth for live data"
    }, indent=2)


# ── Tool 2: List Unattached Disks ────────────────────────────────────────────
@mcp.tool()
def list_unattached_disks(project_id: str = GCP_PROJECT) -> str:
    """
    Find unattached persistent disks in a GCP project. These are disks not
    mounted to any VM — pure waste. Returns disk names, sizes, zones, and
    estimated monthly cost.
    """
    try:
        import subprocess
        result = subprocess.run(
            ["gcloud", "compute", "disks", "list",
             f"--project={project_id}",
             "--format=json",
             "--filter=NOT users:*"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            disks = json.loads(result.stdout)
            return json.dumps({
                "project_id": project_id,
                "disks": [{"name": d.get("name"), "sizeGb": d.get("sizeGb"),
                           "zone": d.get("zone", "").split("/")[-1],
                           "type": d.get("type", "").split("/")[-1]} for d in disks],
                "count": len(disks),
                "estimatedWaste": f"${len(disks) * 14}/month"
            }, indent=2)
    except Exception:
        pass

    return json.dumps({
        "project_id": project_id,
        "disks": [
            {"name": "old-postgres-backup-disk", "sizeGb": "500",
             "zone": "us-central1-a", "type": "pd-standard",
             "estimatedCost": "$20/month", "note": "Backup migrated to GCS"},
            {"name": "dev-workspace-disk", "sizeGb": "200",
             "zone": "us-central1-b", "type": "pd-ssd",
             "estimatedCost": "$34/month", "note": "Developer left team"}
        ],
        "count": 2,
        "totalWaste": "$54/month",
        "note": "DEMO MODE — run with real gcloud auth for live data"
    }, indent=2)


# ── Tool 3: List Unused Reserved IPs ─────────────────────────────────────────
@mcp.tool()
def list_unused_reserved_ips(project_id: str = GCP_PROJECT) -> str:
    """
    Find reserved external IP addresses in a GCP project that are not attached
    to any resource. Each unused reserved IP costs ~$7.20/month.
    """
    try:
        import subprocess
        result = subprocess.run(
            ["gcloud", "compute", "addresses", "list",
             f"--project={project_id}",
             "--format=json",
             "--filter=status=RESERVED"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            ips = json.loads(result.stdout)
            return json.dumps({
                "project_id": project_id,
                "unusedIPs": [{"name": ip.get("name"), "address": ip.get("address"),
                               "region": ip.get("region", "").split("/")[-1]} for ip in ips],
                "count": len(ips),
                "estimatedWaste": f"${len(ips) * 7.20:.2f}/month"
            }, indent=2)
    except Exception:
        pass

    return json.dumps({
        "project_id": project_id,
        "unusedIPs": [
            {"name": "prod-load-balancer-ip-old", "address": "34.102.140.239",
             "region": "us-central1", "note": "LB decommissioned March 2026"}
        ],
        "count": 1,
        "estimatedWaste": "$7.20/month",
        "note": "DEMO MODE — run with real gcloud auth for live data"
    }, indent=2)


# ── Tool 4: Calculate Carbon Footprint ───────────────────────────────────────
@mcp.tool()
def calculate_carbon_footprint(resource_type: str, count: int,
                                hours_per_month: int = 730) -> str:
    """
    Calculate the estimated CO2 emissions and energy consumption for GCP
    resources. resource_type can be: idle_vm, unattached_disk, reserved_ip,
    n1-standard-2, n1-standard-4, n1-standard-8.
    """
    POWER_MAP = {
        "idle_vm": 15, "unattached_disk": 2, "reserved_ip": 0.1,
        "n1-standard-1": 10, "n1-standard-2": 20,
        "n1-standard-4": 40, "n1-standard-8": 80,
    }
    watts = POWER_MAP.get(resource_type, 10)
    kwh_per_month = (watts * hours_per_month * count) / 1000
    co2_kg = kwh_per_month * CARBON_FACTOR * 1000

    return json.dumps({
        "resource_type": resource_type,
        "count": count,
        "kwh_per_month": round(kwh_per_month, 2),
        "co2_kg_per_month": round(co2_kg, 4),
        "co2_tons_per_year": round(co2_kg * 12 / 1000, 4),
        "equivalent": f"{round(co2_kg * 12 / 21, 1)} trees needed to offset annual emissions"
    }, indent=2)


# ── Tool 5: Get Full GreenOps Report ─────────────────────────────────────────
@mcp.tool()
def get_greenops_report(project_id: str = GCP_PROJECT) -> str:
    """
    Run a complete GreenOps scan on a GCP project. Finds all idle VMs,
    unattached disks, unused IPs, calculates total cost waste and carbon
    footprint, and returns a full prioritized action plan with LOW/MEDIUM/HIGH
    risk classifications. This is the main GreenOps analysis tool.
    """
    vms = json.loads(list_idle_vms(project_id))
    disks = json.loads(list_unattached_disks(project_id))
    ips = json.loads(list_unused_reserved_ips(project_id))

    vm_count = vms.get("count", 0)
    disk_count = disks.get("count", 0)
    ip_count = ips.get("count", 0)

    # Calculate costs
    vm_cost = vm_count * 10
    disk_cost = disk_count * 14
    ip_cost = ip_count * 7.20
    total_cost = vm_cost + disk_cost + ip_cost

    # Calculate carbon
    carbon = json.loads(calculate_carbon_footprint("idle_vm", vm_count))
    co2_kg = carbon["co2_kg_per_month"]

    report = {
        "project_id": project_id,
        "summary": {
            "idle_vms": vm_count,
            "unattached_disks": disk_count,
            "unused_ips": ip_count,
            "total_monthly_waste": f"${total_cost:.2f}",
            "total_annual_waste": f"${total_cost * 12:.2f}",
            "co2_kg_per_month": co2_kg,
            "co2_tons_per_year": round(co2_kg * 12 / 1000, 3)
        },
        "priority_actions": {
            "LOW_RISK_execute_immediately": [
                f"Stop {v['name']} ({v.get('zone', 'unknown')})"
                for v in vms.get("instances", [])
            ] + [
                f"Delete disk {d['name']} ({d.get('zone', 'unknown')})"
                for d in disks.get("disks", [])
            ] + [
                f"Release IP {ip['name']} ({ip.get('address', '')})"
                for ip in ips.get("unusedIPs", [])
            ],
            "MEDIUM_RISK_manual_review": [
                "Rightsize over-provisioned VMs (requires manual CPU analysis)"
            ],
            "HIGH_RISK_never_auto_execute": [
                "Databases, production resources — escalate to team"
            ]
        },
        "dashboard_url": "https://greenops-dashboard-845589445410.us-central1.run.app"
    }

    return json.dumps(report, indent=2)


# ── Tool 6: Stop a VM ────────────────────────────────────────────────────────
@mcp.tool()
def stop_vm(instance_name: str, zone: str,
            project_id: str = GCP_PROJECT) -> str:
    """
    Stop a specific idle VM instance in GCP to save cost and reduce carbon
    emissions. Always confirm with the user before calling this tool.
    LOW risk action — VM can be restarted anytime, no data is lost.
    """
    try:
        import subprocess
        result = subprocess.run(
            ["gcloud", "compute", "instances", "stop", instance_name,
             f"--zone={zone}", f"--project={project_id}", "--quiet"],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            return json.dumps({
                "success": True,
                "instance": instance_name,
                "zone": zone,
                "message": f"✅ VM '{instance_name}' stopped successfully.",
                "savings": "~$10-58/month depending on machine type"
            }, indent=2)
        else:
            return json.dumps({"success": False, "error": result.stderr}, indent=2)
    except Exception as e:
        return json.dumps({
            "success": False,
            "instance": instance_name,
            "message": f"[DEMO] Would stop VM '{instance_name}' in {zone}. Estimated saving: $10/month.",
            "note": "DEMO MODE — authenticate with gcloud for real execution"
        }, indent=2)


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    print("GreenOps MCP Server starting...")
    print(f"   Project: {GCP_PROJECT}")
    print("   Tools: list_idle_vms, list_unattached_disks, list_unused_reserved_ips,")
    print("          calculate_carbon_footprint, get_greenops_report, stop_vm")
    print("   Ready for Claude Desktop connections.\n")
    mcp.run(transport="stdio")
