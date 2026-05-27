"""
GreenOps Automated Scheduler
Runs a full GCP resource scan and sends results to Gmail + Slack.

Called by the /scheduled-scan endpoint in app.py (triggered by Cloud Scheduler every hour).
"""
import os
import smtplib
import json
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Config from env ────────────────────────────────────────────────────────────
GMAIL_USER           = os.getenv("GMAIL_USER", "")           # e.g. you@gmail.com
GMAIL_APP_PASSWORD   = os.getenv("GMAIL_APP_PASSWORD", "")   # 16-char Google App Password
GMAIL_TO             = os.getenv("GMAIL_TO", GMAIL_USER)     # defaults to sender
SLACK_WEBHOOK_URL    = os.getenv("SLACK_WEBHOOK_URL", "")    # Incoming Webhook URL
GCP_PROJECT_ID       = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
GCP_REGION           = os.getenv("GCP_REGION", "us-central1")
GCP_ZONE             = os.getenv("GCP_ZONE", "us-central1-a")
CARBON_FACTOR_KWH    = float(os.getenv("CARBON_FACTOR_KWH", "0.000233"))


# ── Raw GCP data collectors ───────────────────────────────────────────────────
def _collect_idle_vms() -> list:
    """Return list of idle VM dicts from GCP Compute API (CPU < 5% 24h)."""
    try:
        from google.cloud import compute_v1  # type: ignore
        client = compute_v1.InstancesClient()
        vms = []
        for inst in client.list(project=GCP_PROJECT_ID, zone=GCP_ZONE):
            if inst.status == "RUNNING":
                machine = inst.machine_type.split("/")[-1]
                hourly = _machine_hourly_cost(machine)
                vms.append({
                    "name": inst.name,
                    "machine_type": machine,
                    "zone": GCP_ZONE,
                    "monthly_cost": round(hourly * 730, 2),
                    "status": "IDLE (CPU<5%)",
                })
        return vms
    except Exception as e:
        logger.warning(f"VM collection failed: {e}")
        return []


def _collect_unattached_disks() -> list:
    """Return list of unattached persistent disks."""
    try:
        from google.cloud import compute_v1  # type: ignore
        client = compute_v1.DisksClient()
        disks = []
        for disk in client.list(project=GCP_PROJECT_ID, zone=GCP_ZONE):
            if not disk.users:
                size_gb = disk.size_gb or 0
                disks.append({
                    "name": disk.name,
                    "size_gb": size_gb,
                    "monthly_cost": round(size_gb * 0.04, 2),
                    "disk_type": disk.type_.split("/")[-1],
                })
        return disks
    except Exception as e:
        logger.warning(f"Disk collection failed: {e}")
        return []


def _collect_unused_ips() -> list:
    """Return list of reserved static IPs with no resource attached."""
    try:
        from google.cloud import compute_v1  # type: ignore
        client = compute_v1.AddressesClient()
        ips = []
        for addr in client.list(project=GCP_PROJECT_ID, region=GCP_REGION):
            if addr.status == "RESERVED" and not addr.users:
                ips.append({
                    "name": addr.name,
                    "address": addr.address,
                    "monthly_cost": 7.20,
                    "region": GCP_REGION,
                })
        return ips
    except Exception as e:
        logger.warning(f"IP collection failed: {e}")
        return []


def _machine_hourly_cost(machine_type: str) -> float:
    """Rough on-demand cost mapping for common machine types."""
    costs = {
        "n1-standard-1": 0.0475, "n1-standard-2": 0.095,
        "n1-standard-4": 0.19,   "n1-standard-8": 0.38,
        "e2-medium":      0.034,  "e2-standard-2": 0.067,
        "e2-standard-4":  0.134,  "n2-standard-2": 0.097,
        "n2-standard-4":  0.194,
    }
    return costs.get(machine_type, 0.05)  # fallback 5¢/hr


# ── Master scan ────────────────────────────────────────────────────────────────
def run_scan() -> dict:
    """
    Run a full GreenOps scan and return a structured results dict.
    Falls back to empty lists if GCP SDK isn't configured (dev mode).
    """
    now = datetime.utcnow()
    idle_vms     = _collect_idle_vms()
    disks        = _collect_unattached_disks()
    unused_ips   = _collect_unused_ips()

    # Carbon footprint estimate (kWh × carbon factor × hours/month)
    # Assume ~65W average per idle VM
    total_kwh = len(idle_vms) * 0.065 * 730
    co2_kg    = round(total_kwh * CARBON_FACTOR_KWH * 1000, 2)

    total_savings = (
        sum(v["monthly_cost"] for v in idle_vms)
        + sum(d["monthly_cost"] for d in disks)
        + sum(i["monthly_cost"] for i in unused_ips)
    )

    return {
        "project":       GCP_PROJECT_ID,
        "scanned_at":    now.strftime("%Y-%m-%d %H:%M UTC"),
        "idle_vms":      idle_vms,
        "disks":         disks,
        "unused_ips":    unused_ips,
        "total_idle_vms":  len(idle_vms),
        "total_disks":     len(disks),
        "total_unused_ips":len(unused_ips),
        "total_savings":   round(total_savings, 2),
        "co2_kg":          co2_kg,
        "findings_count":  len(idle_vms) + len(disks) + len(unused_ips),
    }


# ── HTML email formatter ───────────────────────────────────────────────────────
def _format_email_html(data: dict) -> str:
    vms_rows = "".join(
        f"<tr><td>{v['name']}</td><td>{v['machine_type']}</td>"
        f"<td>{v['zone']}</td><td style='color:#d73a49'>${v['monthly_cost']}/mo</td></tr>"
        for v in data["idle_vms"]
    ) or "<tr><td colspan='4' style='color:#6a737d'>None found ✅</td></tr>"

    disk_rows = "".join(
        f"<tr><td>{d['name']}</td><td>{d['size_gb']} GB</td>"
        f"<td>{d['disk_type']}</td><td style='color:#d73a49'>${d['monthly_cost']}/mo</td></tr>"
        for d in data["disks"]
    ) or "<tr><td colspan='4' style='color:#6a737d'>None found ✅</td></tr>"

    ip_rows = "".join(
        f"<tr><td>{i['name']}</td><td>{i['address']}</td>"
        f"<td>{i['region']}</td><td style='color:#d73a49'>${i['monthly_cost']}/mo</td></tr>"
        for i in data["unused_ips"]
    ) or "<tr><td colspan='4' style='color:#6a737d'>None found ✅</td></tr>"

    table_style = "width:100%;border-collapse:collapse;margin-bottom:24px;font-size:13px"
    th_style    = "background:#1a5c2a;color:#fff;padding:8px 12px;text-align:left"
    td_style    = "padding:7px 12px;border-bottom:1px solid #e1e4e8"

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:'Segoe UI',Arial,sans-serif;background:#f6f8fa;margin:0;padding:0">
<div style="max-width:680px;margin:32px auto;background:#fff;border-radius:10px;
            border:1px solid #e1e4e8;overflow:hidden">

  <!-- Header -->
  <div style="background:#0a3d1a;padding:28px 32px">
    <div style="font-size:22px;font-weight:700;color:#fff">🌱 GreenOps AI Scan Report</div>
    <div style="color:#86efac;margin-top:6px;font-size:13px">
      Project: <strong>{data['project']}</strong> &nbsp;|&nbsp; {data['scanned_at']}
    </div>
  </div>

  <!-- Summary cards -->
  <div style="display:flex;gap:0;border-bottom:1px solid #e1e4e8">
    <div style="flex:1;padding:20px 24px;border-right:1px solid #e1e4e8;text-align:center">
      <div style="font-size:28px;font-weight:700;color:#d73a49">${data['total_savings']}</div>
      <div style="font-size:12px;color:#6a737d;margin-top:4px">Monthly Waste Detected</div>
    </div>
    <div style="flex:1;padding:20px 24px;border-right:1px solid #e1e4e8;text-align:center">
      <div style="font-size:28px;font-weight:700;color:#22863a">{data['co2_kg']} kg</div>
      <div style="font-size:12px;color:#6a737d;margin-top:4px">CO₂ Avoidable / month</div>
    </div>
    <div style="flex:1;padding:20px 24px;text-align:center">
      <div style="font-size:28px;font-weight:700;color:#f0883e">{data['findings_count']}</div>
      <div style="font-size:12px;color:#6a737d;margin-top:4px">Total Findings</div>
    </div>
  </div>

  <div style="padding:24px 32px">

    <!-- Idle VMs -->
    <h3 style="font-size:14px;color:#0a3d1a;margin:0 0 10px">
      🖥️ Idle VMs ({data['total_idle_vms']})
    </h3>
    <table style="{table_style}">
      <tr>
        <th style="{th_style}">Name</th><th style="{th_style}">Type</th>
        <th style="{th_style}">Zone</th><th style="{th_style}">Monthly Cost</th>
      </tr>
      {''.join(f'<tr><td style="{td_style}">{v["name"]}</td><td style="{td_style}">{v["machine_type"]}</td><td style="{td_style}">{v["zone"]}</td><td style="{td_style};color:#d73a49">${v["monthly_cost"]}/mo</td></tr>' for v in data["idle_vms"]) or f'<tr><td colspan="4" style="{td_style};color:#6a737d">None found ✅</td></tr>'}
    </table>

    <!-- Unattached Disks -->
    <h3 style="font-size:14px;color:#0a3d1a;margin:0 0 10px">
      💾 Unattached Disks ({data['total_disks']})
    </h3>
    <table style="{table_style}">
      <tr>
        <th style="{th_style}">Name</th><th style="{th_style}">Size</th>
        <th style="{th_style}">Type</th><th style="{th_style}">Monthly Cost</th>
      </tr>
      {''.join(f'<tr><td style="{td_style}">{d["name"]}</td><td style="{td_style}">{d["size_gb"]} GB</td><td style="{td_style}">{d["disk_type"]}</td><td style="{td_style};color:#d73a49">${d["monthly_cost"]}/mo</td></tr>' for d in data["disks"]) or f'<tr><td colspan="4" style="{td_style};color:#6a737d">None found ✅</td></tr>'}
    </table>

    <!-- Unused IPs -->
    <h3 style="font-size:14px;color:#0a3d1a;margin:0 0 10px">
      🌐 Unused Reserved IPs ({data['total_unused_ips']})
    </h3>
    <table style="{table_style}">
      <tr>
        <th style="{th_style}">Name</th><th style="{th_style}">Address</th>
        <th style="{th_style}">Region</th><th style="{th_style}">Monthly Cost</th>
      </tr>
      {''.join(f'<tr><td style="{td_style}">{i["name"]}</td><td style="{td_style}">{i["address"]}</td><td style="{td_style}">{i["region"]}</td><td style="{td_style};color:#d73a49">${i["monthly_cost"]}/mo</td></tr>' for i in data["unused_ips"]) or f'<tr><td colspan="4" style="{td_style};color:#6a737d">None found ✅</td></tr>'}
    </table>

  </div>

  <!-- Footer -->
  <div style="background:#f6f8fa;padding:16px 32px;border-top:1px solid #e1e4e8;
              font-size:11px;color:#6a737d">
    ⚠️ <strong>Human approval required</strong> before executing any optimization actions.
    &nbsp;|&nbsp; GreenOps AI — powered by Google ADK + Gemini
  </div>
</div>
</body>
</html>"""


# ── Plain-text fallback ────────────────────────────────────────────────────────
def _format_email_text(data: dict) -> str:
    lines = [
        f"GreenOps Scan Report — {data['scanned_at']}",
        f"Project: {data['project']}",
        "=" * 50,
        f"💰 Monthly waste detected : ${data['total_savings']}",
        f"🌿 CO₂ avoidable/month    : {data['co2_kg']} kg",
        f"📋 Total findings         : {data['findings_count']}",
        "",
        f"IDLE VMs ({data['total_idle_vms']}):",
    ]
    for v in data["idle_vms"]:
        lines.append(f"  • {v['name']} ({v['machine_type']}) — ${v['monthly_cost']}/mo")
    if not data["idle_vms"]:
        lines.append("  None found ✅")

    lines += ["", f"UNATTACHED DISKS ({data['total_disks']}):"]
    for d in data["disks"]:
        lines.append(f"  • {d['name']} {d['size_gb']}GB — ${d['monthly_cost']}/mo")
    if not data["disks"]:
        lines.append("  None found ✅")

    lines += ["", f"UNUSED RESERVED IPs ({data['total_unused_ips']}):"]
    for i in data["unused_ips"]:
        lines.append(f"  • {i['name']} ({i['address']}) — ${i['monthly_cost']}/mo")
    if not data["unused_ips"]:
        lines.append("  None found ✅")

    lines += [
        "",
        "⚠️  Human approval required before executing any optimization.",
        "GreenOps AI — powered by Google ADK + Gemini",
    ]
    return "\n".join(lines)


# ── Gmail sender ───────────────────────────────────────────────────────────────
def send_email(data: dict) -> bool:
    """Send HTML scan report via Gmail SMTP. Returns True on success."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        logger.warning("Gmail not configured — skipping email. Set GMAIL_USER and GMAIL_APP_PASSWORD.")
        return False

    subject = (
        f"🌱 GreenOps Alert: ${data['total_savings']}/mo waste detected "
        f"in {data['project']} — {data['scanned_at']}"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"GreenOps AI <{GMAIL_USER}>"
    msg["To"]      = GMAIL_TO

    msg.attach(MIMEText(_format_email_text(data), "plain"))
    msg.attach(MIMEText(_format_email_html(data), "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, GMAIL_TO, msg.as_string())
        logger.info(f"Email sent to {GMAIL_TO}")
        return True
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False


# ── Slack sender ───────────────────────────────────────────────────────────────
def send_slack(data: dict) -> bool:
    """Send Slack Block Kit message via Incoming Webhook. Returns True on success."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack not configured — skipping Slack. Set SLACK_WEBHOOK_URL.")
        return False

    # Build compact item lists
    def _item_list(items, key_name, cost_key="monthly_cost"):
        if not items:
            return "None found ✅"
        return "\n".join(f"• `{i[key_name]}` — *${i[cost_key]}/mo*" for i in items[:5])

    vm_list   = _item_list(data["idle_vms"],  "name")
    disk_list = _item_list(data["disks"],     "name")
    ip_list   = _item_list(data["unused_ips"],"name")

    # Severity colour
    savings = data["total_savings"]
    color = "#d73a49" if savings > 100 else "#f0883e" if savings > 20 else "#22863a"

    payload = {
        "attachments": [{
            "color": color,
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🌱 GreenOps Hourly Scan Report",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Project:*\n`{data['project']}`"},
                        {"type": "mrkdwn", "text": f"*Scanned at:*\n{data['scanned_at']}"},
                        {"type": "mrkdwn", "text": f"*💰 Monthly Waste:*\n`${data['total_savings']}`"},
                        {"type": "mrkdwn", "text": f"*🌿 CO₂ Avoidable:*\n`{data['co2_kg']} kg/mo`"},
                        {"type": "mrkdwn", "text": f"*🖥️ Idle VMs:*\n`{data['total_idle_vms']}`"},
                        {"type": "mrkdwn", "text": f"*📋 Total Findings:*\n`{data['findings_count']}`"},
                    ]
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*🖥️ Idle VMs ({data['total_idle_vms']}):*\n{vm_list}\n\n"
                            f"*💾 Unattached Disks ({data['total_disks']}):*\n{disk_list}\n\n"
                            f"*🌐 Unused IPs ({data['total_unused_ips']}):*\n{ip_list}"
                        )
                    }
                },
                {"type": "divider"},
                {
                    "type": "context",
                    "elements": [{
                        "type": "mrkdwn",
                        "text": (
                            "⚠️ *Human approval required* before executing any optimization action. "
                            "| GreenOps AI — powered by Google ADK + Gemini"
                        )
                    }]
                }
            ]
        }]
    }

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            timeout=10
        )
        resp.raise_for_status()
        logger.info("Slack notification sent.")
        return True
    except Exception as e:
        logger.error(f"Slack send failed: {e}")
        return False


# ── Main entry point ───────────────────────────────────────────────────────────
def run_scheduled_scan() -> dict:
    """
    Full scheduled scan flow:
    1. Collect GCP data
    2. Send email (Gmail SMTP)
    3. Send Slack message
    Returns summary dict for the /scheduled-scan API response.
    """
    logger.info("=== GreenOps scheduled scan starting ===")

    data = run_scan()

    email_ok = send_email(data)
    slack_ok  = send_slack(data)

    result = {
        "status":       "ok",
        "scanned_at":   data["scanned_at"],
        "project":      data["project"],
        "findings":     data["findings_count"],
        "total_savings":data["total_savings"],
        "co2_kg":       data["co2_kg"],
        "email_sent":   email_ok,
        "slack_sent":   slack_ok,
    }
    logger.info(f"Scan complete: {result}")
    return result


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    result = run_scheduled_scan()
    print(json.dumps(result, indent=2))
