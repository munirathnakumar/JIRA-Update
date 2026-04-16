"""
JIRA Bulk Updater
-----------------
Reads an  sheet, maps columns to JIRA fields via config.yaml,
and updates each JIRA issue using the JIRA REST API v3.

Usage:
    python update_jira.py                        # uses config.yaml in same directory
    python update_jira.py --config my_config.yaml
    python update_jira.py --dry-run              # preview without updating
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import requests
import yaml
from requests.auth import HTTPBasicAuth


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def setup_logging(log_file: str, log_level: str) -> logging.Logger:
    level = getattr(logging, log_level.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    return logging.getLogger("jira_updater")


def build_auth(cfg: dict) -> HTTPBasicAuth:
    return HTTPBasicAuth(cfg["jira"]["username"], cfg["jira"]["api_token"])


def get_jira_issue(base_url: str, auth: HTTPBasicAuth, issue_key: str) -> dict | None:
    """Fetch the current state of a JIRA issue."""
    url = f"{base_url}/rest/api/3/issue/{issue_key}"
    resp = requests.get(url, auth=auth, headers={"Accept": "application/json"}, timeout=15)
    if resp.status_code == 200:
        return resp.json()
    return None


def resolve_assignee_account_id(
    base_url: str, auth: HTTPBasicAuth, identifier: str
) -> str | None:
    """
    Resolve an email address to a JIRA account ID.
    If identifier looks like an account ID already (no '@'), return as-is.
    """
    if "@" not in identifier:
        return identifier
    url = f"{base_url}/rest/api/3/user/search"
    resp = requests.get(
        url,
        auth=auth,
        headers={"Accept": "application/json"},
        params={"query": identifier},
        timeout=15,
    )
    if resp.status_code == 200:
        users = resp.json()
        if users:
            return users[0]["accountId"]
    return None


# ---------------------------------------------------------------------------
# Field value builders
# ---------------------------------------------------------------------------

# Map of well-known logical field names to their JIRA REST field names / IDs
FIELD_ALIASES = {
    "story_points": "customfield_10016",
}


def _make_doc(text: str) -> dict:
    """Wrap plain text in an Atlassian Document Format (ADF) paragraph."""
    return {
        "version": 1,
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": text}]}
        ],
    }


def build_field_entry(
    field_name: str,
    excel_value,
    update_mode: str,
    current_issue: dict,
    field_type: str = "text",
) -> tuple[dict, dict]:
    """
    Return (fields_fragment, update_fragment) to be merged into the request body:
      {"fields": {<fields_fragment>}, "update": {<update_fragment>}}

    JIRA REST API v3 rules:
    - `fields`  dict  → direct value set; works for ALL field types (text, select, number…)
    - `update`  dict  → operation list [{add/remove/set}]; needed only for multi-value
                        system fields (labels, components) in append mode.

    field_type values:
      "text"         — plain string (default)
      "select"       — single-select dropdown  → {"value": "..."}
      "radio"        — radio button            → {"value": "..."}  (same as select)
      "multi_select" — multi-select dropdown   → [{"value": "..."}, ...]
      "checkbox"     — checkbox field          → [{"value": "..."}, ...]  (same as multi_select)
      "number"       — numeric custom field
      "date"         — date field  YYYY-MM-DD
    """
    jira_key = FIELD_ALIASES.get(field_name, field_name)
    str_value = str(excel_value).strip()

    fields: dict = {}
    update_ops: dict = {}

    # ---------- summary ----------
    if jira_key == "summary":
        fields[jira_key] = str_value

    # ---------- description ----------
    elif jira_key == "description":
        if update_mode == "append":
            existing = ""
            try:
                existing = (
                    current_issue["fields"]["description"]
                    ["content"][0]["content"][0]["text"]
                )
            except (KeyError, TypeError, IndexError):
                pass
            str_value = f"{existing}\n{str_value}".strip()
        fields[jira_key] = _make_doc(str_value)

    # ---------- priority (built-in single-select) ----------
    elif jira_key == "priority":
        fields[jira_key] = {"name": str_value}

    # ---------- assignee ----------
    elif jira_key == "assignee":
        fields[jira_key] = {"accountId": str_value}

    # ---------- labels ----------
    elif jira_key == "labels":
        new_labels = [lbl.strip() for lbl in str_value.split(",") if lbl.strip()]
        if update_mode == "append":
            # Use update-ops so we only add, not overwrite
            update_ops[jira_key] = [{"add": lbl} for lbl in new_labels]
        else:
            fields[jira_key] = new_labels

    # ---------- components ----------
    elif jira_key == "components":
        names = [c.strip() for c in str_value.split(",") if c.strip()]
        if update_mode == "append":
            update_ops[jira_key] = [{"add": {"name": n}} for n in names]
        else:
            fields[jira_key] = [{"name": n} for n in names]

    # ---------- story points ----------
    elif jira_key == "customfield_10016":
        try:
            fields[jira_key] = float(str_value)
        except ValueError:
            raise ValueError(f"Story points value '{str_value}' is not a number")

    # ---------- single-select / radio (custom field) ----------
    elif field_type in ("select", "radio"):
        fields[jira_key] = {"value": str_value}

    # ---------- multi-select / checkbox (custom field) ----------
    elif field_type in ("multi_select", "checkbox"):
        options = [v.strip() for v in str_value.split(",") if v.strip()]
        if update_mode == "append":
            existing_vals = [
                o.get("value", "")
                for o in (current_issue.get("fields", {}).get(jira_key) or [])
            ]
            options = list(dict.fromkeys(existing_vals + options))
        fields[jira_key] = [{"value": v} for v in options]

    # ---------- number ----------
    elif field_type == "number":
        try:
            fields[jira_key] = float(str_value)
        except ValueError:
            raise ValueError(f"Expected a number for field '{jira_key}', got '{str_value}'")

    # ---------- date ----------
    elif field_type == "date":
        fields[jira_key] = str_value  # YYYY-MM-DD

    # ---------- generic text / custom field ----------
    else:
        fields[jira_key] = str_value

    return fields, update_ops


# ---------------------------------------------------------------------------
# Core updater
# ---------------------------------------------------------------------------

def update_issue(
    base_url: str,
    auth: HTTPBasicAuth,
    issue_key: str,
    fields_payload: dict,
    update_payload: dict,
    logger: logging.Logger,
    dry_run: bool,
) -> bool:
    """PUT to JIRA with separate `fields` (replace) and `update` (append ops) dicts."""
    url = f"{base_url}/rest/api/3/issue/{issue_key}"
    body: dict = {}
    if fields_payload:
        body["fields"] = fields_payload
    if update_payload:
        body["update"] = update_payload

    if dry_run:
        logger.info("[DRY-RUN] Would update %s with: %s", issue_key, body)
        return True

    resp = requests.put(
        url,
        auth=auth,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json=body,
        timeout=15,
    )

    if resp.status_code in (200, 204):
        return True

    logger.error(
        "Failed to update %s: HTTP %s — %s", issue_key, resp.status_code, resp.text
    )
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Bulk-update JIRA issues from an Excel file")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent / "config.yaml",
        help="Path to the YAML config file (default: config.yaml next to this script)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview updates without writing to JIRA",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    settings = cfg.get("settings", {})
    dry_run = args.dry_run or settings.get("dry_run", False)

    logger = setup_logging(
        settings.get("log_file", "jira_update.log"),
        settings.get("log_level", "INFO"),
    )
    logger.info("Starting JIRA updater%s", " [DRY-RUN]" if dry_run else "")

    # Resolve Excel path relative to config file
    excel_path = Path(cfg["excel"]["file_path"])
    if not excel_path.is_absolute():
        excel_path = args.config.parent / excel_path

    sheet = cfg["excel"].get("sheet_name") or 0
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet, dtype=str)
    except FileNotFoundError:
        logger.error("Excel file not found: %s", excel_path)
        sys.exit(1)

    jira_id_col = cfg["jira_id_column"]
    if jira_id_col not in df.columns:
        logger.error("Column '%s' not found in Excel. Available: %s", jira_id_col, list(df.columns))
        sys.exit(1)

    field_mappings = cfg.get("field_mappings", [])
    skip_empty = settings.get("skip_empty_cells", True)

    base_url = cfg["jira"]["base_url"].rstrip("/")
    auth = build_auth(cfg)

    success_count = error_count = skip_count = 0

    for row_idx, row in df.iterrows():
        issue_key = str(row[jira_id_col]).strip() if pd.notna(row[jira_id_col]) else ""
        if not issue_key or issue_key.lower() == "nan":
            logger.warning("Row %d: empty JIRA ID — skipped", row_idx + 2)
            skip_count += 1
            continue

        logger.info("Processing %s (row %d) …", issue_key, row_idx + 2)

        # Fetch current issue state (needed for append mode & assignee resolution)
        current_issue = get_jira_issue(base_url, auth, issue_key)
        if current_issue is None and not dry_run:
            logger.error("  Issue %s not found or inaccessible — skipped", issue_key)
            error_count += 1
            continue

        fields_payload: dict = {}
        update_payload: dict = {}

        for mapping in field_mappings:
            excel_col = mapping["excel_column"]
            jira_field = mapping["jira_field"]
            update_mode = mapping.get("update_mode", "replace")
            field_type = mapping.get("field_type", "text")

            if excel_col not in df.columns:
                logger.warning("  Column '%s' not found in Excel — skipped", excel_col)
                continue

            cell_value = row.get(excel_col)
            if skip_empty and (pd.isna(cell_value) or str(cell_value).strip() == ""):
                logger.debug("  Skipping empty cell for column '%s'", excel_col)
                continue

            try:
                # Special handling: resolve assignee email → account ID
                if jira_field == "assignee":
                    resolved = resolve_assignee_account_id(base_url, auth, str(cell_value).strip())
                    if resolved is None:
                        logger.warning("  Could not resolve assignee '%s' — skipped", cell_value)
                        continue
                    cell_value = resolved

                f_frag, u_frag = build_field_entry(
                    jira_field, cell_value, update_mode, current_issue or {}, field_type
                )
                fields_payload.update(f_frag)
                update_payload.update(u_frag)
            except Exception as exc:
                logger.error("  Error building payload for field '%s': %s", jira_field, exc)

        if not fields_payload and not update_payload:
            logger.info("  No fields to update for %s — skipped", issue_key)
            skip_count += 1
            continue

        ok = update_issue(base_url, auth, issue_key, fields_payload, update_payload, logger, dry_run)
        if ok:
            logger.info("  %s updated successfully", issue_key)
            success_count += 1
        else:
            error_count += 1

    logger.info(
        "Done. Success: %d | Errors: %d | Skipped: %d",
        success_count, error_count, skip_count,
    )


if __name__ == "__main__":
    main()
