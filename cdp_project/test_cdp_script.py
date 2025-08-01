from cdp_secure.encrypt_decrypt import CDPCredentialsHandler
import json

# ====== CONFIG ======
ENVIRONMENT_NAME = "cdp-infra2-cdp"
DATALAKE_NAME = "cdp-infra2-cdp-dlake-mzh9"
DATAHUB_CLUSTER_NAME = "cod-328bf4e7hjof"

# ====== FILE PATH VARIABLES (only visible here) ======
ENC_PATH = r"C:\Users\vutuk\.cdp\credentials.enc"
KEY_PATH = r"C:\Users\vutuk\.cdp\secret.key"

# ====== INIT HANDLER (pass encrypted and key path) ======
cdp_handler = CDPCredentialsHandler(enc_path=ENC_PATH, key_path=KEY_PATH)

# Step 1: Encrypt if needed
cdp_handler.encrypt_if_needed()

# Step 2: Decrypt and prepare credentials
cdp_handler.decrypt_and_prepare_credentials()

# ====== CDP COMMAND HELPERS ======
def get_latest_event(events):
    return sorted(events, key=lambda e: e["eventTimestamp"], reverse=True)[0] if events else {}

def format_event(event):
    return f'{event.get("eventTimestamp", "Unknown")} - {event.get("eventMessage", "No message")}'

def is_healthy_event(event):
    return event.get("eventType") == "AVAILABLE"

def run_json(command):
    result = cdp_handler.run_cdp_command(command)
    if result.returncode != 0:
        return {"error": result.stderr.strip()}
    return json.loads(result.stdout)

def get_freeipa_status(env_name):
    status_resp = run_json(["cdp", "environments", "get-freeipa-status", "--environment-name", env_name])
    if "error" in status_resp:
        return "Error", status_resp["error"]

    status = status_resp.get("status", "UNKNOWN")
    instances = status_resp.get("instances", {})

    problem_nodes = []
    for host, data in instances.items():
        node_status = data.get("status", "UNKNOWN")
        issues = data.get("issues", [])
        if node_status != "CREATED" or issues:
            issue_text = f" ({', '.join(issues)})" if issues else ""
            problem_nodes.append(f"{host}: {node_status}{issue_text}")

    reason = "; ".join(problem_nodes) if problem_nodes else "N/A"
    return status, reason

def get_datalake_status(datalake_name):
    status_resp = run_json(["cdp", "datalake", "describe-datalake", "--datalake-name", datalake_name])
    if "error" in status_resp:
        return "Error", status_resp["error"]

    datalake = status_resp.get("datalake", {})
    status = datalake.get("status", "UNKNOWN")
    instance_groups = datalake.get("instanceGroups", [])

    unhealthy_instances = []
    for group in instance_groups:
        group_name = group.get("name", "UnknownGroup")
        for instance in group.get("instances", []):
            fqdn = instance.get("discoveryFQDN", "unknown-host")
            service_status = instance.get("instanceStatus", "UNKNOWN")
            reason = instance.get("statusReason", "No reason")
            if service_status != "SERVICES_HEALTHY":
                unhealthy_instances.append(f"{fqdn} ({group_name}): {service_status} - {reason}")

    reason = "; ".join(unhealthy_instances) if unhealthy_instances else "N/A"
    return status, reason

def get_datahub_status(cluster_name):
    desc = run_json(["cdp", "datahub", "describe-cluster", "--cluster-name", cluster_name])
    if "error" in desc:
        return "Error", desc["error"]

    status = desc.get("cluster", {}).get("status", "UNKNOWN")
    events_resp = run_json(["cdp", "datahub", "list-cluster-lifecycle-events", "--cluster", cluster_name])
    if "error" in events_resp:
        return status, "N/A"

    events = events_resp.get("clusterEvents", [])
    latest_event = get_latest_event(events)

    if is_healthy_event(latest_event):
        return status, "N/A"
    else:
        return status, format_event(latest_event)

# ====== MAIN ======
if __name__ == "__main__":
    print("=== CDP Health Check ===")

    ipa_status, ipa_reason = get_freeipa_status(ENVIRONMENT_NAME)
    print(f"FreeIPA Status: {ipa_status}")
    print(f"Reason: {ipa_reason}\n")

    dl_status, dl_reason = get_datalake_status(DATALAKE_NAME)
    print(f"Datalake Status: {dl_status}")
    print(f"Reason: {dl_reason}\n")

    dh_status, dh_reason = get_datahub_status(DATAHUB_CLUSTER_NAME)
    print(f"DataHub Status: {dh_status}")
    print(f"Reason: {dh_reason}\n")

    cdp_handler.cleanup_credentials_file()
