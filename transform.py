#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


DEFAULT_CONFIG = "config.json"
DEFAULT_INPUT_DIR = "./auth-dir"
DEFAULT_OUTPUT_FILE = "sub2api_accounts_import.json"
DEFAULT_REPORT_FILE = "sub2api_dedupe_report.json"
DEFAULT_INCLUDE_PATTERN = "*@*.json"
DEFAULT_PAGE_SIZE = 100
DEFAULT_TIMEOUT = 15
DEFAULT_PLATFORM = "openai"
DEFAULT_ACCOUNT_TYPE = "oauth"
DEFAULT_CONCURRENCY = 3
DEFAULT_PRIORITY = 50
DEFAULT_NAME_SOURCE = "email"
DEFAULT_NAME_PREFIX = "acc"
DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_ACCOUNTS_PATH = "/api/v1/admin/accounts"

LOGGER = logging.getLogger("transform")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_config(path: str) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return {}
    data = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"config file must be a JSON object: {config_path}")
    return data


def conf_get(conf: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = conf.get(key)
        if value not in (None, ""):
            return value
    return default


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in {0, 1}:
            return bool(value)
        raise RuntimeError(f"invalid boolean value: {value!r}")
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    raise RuntimeError(f"invalid boolean value: {value!r}")


def maybe_json_loads(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def compact_text(text: Any, limit: int = 240) -> str | None:
    if text is None:
        return None
    text = str(text).replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return None
    return text if len(text) <= limit else text[: max(0, limit - 3)] + "..."


def normalize_authorization(value: str) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    return token if token.lower().startswith("bearer ") else f"Bearer {token}"


def http_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": normalize_authorization(token),
        "Accept": "application/json, text/plain, */*",
    }


def request_text(url: str, *, headers: dict[str, str], timeout: int) -> tuple[int, str]:
    request = urllib.request.Request(url=url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as exc:
        raise RuntimeError(str(getattr(exc, "reason", exc))) from exc


def configure_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def collect_json_files(
    input_dir: Path,
    output_file: Path,
    report_file: Path,
    recursive: bool,
    include_pattern: str,
    exclude_patterns: list[str],
) -> list[Path]:
    files = sorted(input_dir.rglob(include_pattern) if recursive else input_dir.glob(include_pattern))
    output_resolved = output_file.resolve()
    report_resolved = report_file.resolve()
    result: list[Path] = []
    for path in files:
        if not path.is_file():
            continue
        resolved = path.resolve()
        if resolved in {output_resolved, report_resolved}:
            continue
        if any(fnmatch.fnmatch(path.name, pattern) for pattern in exclude_patterns):
            continue
        result.append(path)
    return result


def normalize_records(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    return [data]


def nested_dicts(item: dict[str, Any]) -> list[dict[str, Any]]:
    result = [item]
    for key in ("credentials", "credential", "profile", "auth", "metadata"):
        value = item.get(key)
        if isinstance(value, dict):
            result.append(value)
    return result


def normalize_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text


def extract_email_from_sources(sources: list[dict[str, Any]]) -> str:
    for source in sources:
        for key in ("email", "account", "mail", "username", "login", "identifier", "name"):
            value = source.get(key)
            email = normalize_email(value)
            if email and "@" in email:
                return email
    return ""


def extract_local_email(credentials: dict[str, Any], path: Path) -> str:
    email = extract_email_from_sources(nested_dicts(credentials))
    if email:
        return email
    stem_email = normalize_email(path.stem)
    return stem_email if "@" in stem_email else ""


def extract_remote_email(item: dict[str, Any]) -> str:
    return extract_email_from_sources(nested_dicts(item))


def choose_name(
    credentials: dict[str, Any],
    path: Path,
    index: int,
    name_source: str,
    name_prefix: str,
    email: str,
) -> str:
    if name_source == "index":
        return f"{name_prefix}-{index:03d}"
    if name_source == "email":
        if email:
            return email
        account_id = str(credentials.get("account_id", "")).strip()
        if account_id:
            return account_id
    return path.stem


def dedupe_name(name: str, used: dict[str, int]) -> str:
    current = used.get(name, 0) + 1
    used[name] = current
    if current == 1:
        return name
    return f"{name}-{current}"


def looks_like_account_item(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    direct_keys = {"email", "account", "name", "platform", "type", "status", "credentials"}
    if any(key in item for key in direct_keys):
        return True
    return any(isinstance(item.get(key), dict) for key in ("credentials", "profile", "auth"))


def find_account_list(node: Any, depth: int = 0) -> list[dict[str, Any]] | None:
    if depth > 5:
        return None
    if isinstance(node, list):
        rows = [row for row in node if isinstance(row, dict)]
        if rows and any(looks_like_account_item(row) for row in rows):
            return rows
        for item in node:
            found = find_account_list(item, depth + 1)
            if found is not None:
                return found
        return None
    if not isinstance(node, dict):
        return None

    for key in ("items", "list", "rows", "records", "accounts", "results", "data"):
        value = node.get(key)
        if isinstance(value, list):
            rows = [row for row in value if isinstance(row, dict)]
            if rows and any(looks_like_account_item(row) for row in rows):
                return rows

    for key in ("data", "result", "payload", "response"):
        value = node.get(key)
        found = find_account_list(value, depth + 1)
        if found is not None:
            return found

    for value in node.values():
        if isinstance(value, (dict, list)):
            found = find_account_list(value, depth + 1)
            if found is not None:
                return found
    return None


def find_pagination_value(node: Any, keys: tuple[str, ...], depth: int = 0) -> int | None:
    if depth > 5:
        return None
    if isinstance(node, dict):
        for key in keys:
            value = node.get(key)
            if isinstance(value, bool):
                continue
            try:
                if value is not None:
                    return int(value)
            except (TypeError, ValueError):
                pass
        for nested_key in ("pagination", "pager", "meta", "data", "result"):
            nested = node.get(nested_key)
            found = find_pagination_value(nested, keys, depth + 1)
            if found is not None:
                return found
        for value in node.values():
            if isinstance(value, (dict, list)):
                found = find_pagination_value(value, keys, depth + 1)
                if found is not None:
                    return found
    elif isinstance(node, list):
        for item in node:
            found = find_pagination_value(item, keys, depth + 1)
            if found is not None:
                return found
    return None


def build_accounts_url(settings: dict[str, Any], page: int) -> str:
    base_url = str(settings.get("sub2api_accounts_url") or "").strip()
    if not base_url:
        root = str(settings.get("sub2api_base_url") or "").strip().rstrip("/")
        if not root:
            raise RuntimeError("missing sub2api base URL")
        base_url = root + DEFAULT_ACCOUNTS_PATH
    params = {
        "page": page,
        "page_size": settings["sub2api_page_size"],
        "platform": settings["sub2api_platform_filter"],
        "type": settings["sub2api_type_filter"],
        "status": settings["sub2api_status_filter"],
        "privacy_mode": settings["sub2api_privacy_mode_filter"],
        "group": settings["sub2api_group_filter"],
        "search": settings["sub2api_search_filter"],
        "lite": 1 if settings["sub2api_lite"] else 0,
        "timezone": settings["sub2api_timezone"],
    }
    return f"{base_url}?{urlencode(params)}"


def fetch_sub2api_accounts(settings: dict[str, Any]) -> list[dict[str, Any]]:
    page = 1
    accounts: list[dict[str, Any]] = []
    while True:
        url = build_accounts_url(settings, page)
        status, text = request_text(url, headers=http_headers(settings["sub2api_token"]), timeout=settings["timeout"])
        if status >= 400:
            raise RuntimeError(f"sub2api accounts request failed: http {status} | {compact_text(text, 300) or '-'}")

        payload = maybe_json_loads(text)
        if not isinstance(payload, (dict, list)):
            raise RuntimeError("sub2api accounts response is not valid JSON")

        page_items = find_account_list(payload) or []
        if page == 1:
            LOGGER.info("fetched sub2api page %s: %s accounts", page, len(page_items))
        else:
            LOGGER.info("fetched sub2api page %s: %s accounts (running total: %s)", page, len(page_items), len(accounts) + len(page_items))

        if not page_items:
            break

        accounts.extend(page_items)

        total_pages = find_pagination_value(payload, ("page_count", "pages", "total_pages", "last_page"))
        if total_pages is not None and page >= total_pages:
            break
        if len(page_items) < settings["sub2api_page_size"]:
            break
        page += 1
    return accounts


def fetch_existing_remote_emails(settings: dict[str, Any]) -> tuple[list[dict[str, Any]], set[str]]:
    accounts = fetch_sub2api_accounts(settings)
    emails = {email for email in (extract_remote_email(item) for item in accounts) if email}
    LOGGER.info("fetched remote accounts: %s, remote emails: %s", len(accounts), len(emails))
    return accounts, emails


def build_account_entry(
    credentials: dict[str, Any],
    path: Path,
    counter: int,
    used_names: dict[str, int],
    settings: dict[str, Any],
    email: str,
) -> dict[str, Any]:
    base_name = choose_name(credentials, path, counter, settings["name_source"], settings["name_prefix"], email)
    name = dedupe_name(base_name, used_names)
    return {
        "name": name,
        "platform": settings["platform"],
        "type": settings["account_type"],
        "credentials": credentials,
        "concurrency": settings["concurrency"],
        "priority": settings["priority"],
    }


def build_import_payload(accounts: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "type": "sub2api-data",
        "version": 1,
        "exported_at": utc_now_iso(),
        "proxies": [],
        "accounts": accounts,
    }


def build_report(
    *,
    settings: dict[str, Any],
    files: list[Path],
    remote_accounts: list[dict[str, Any]],
    remote_emails: set[str],
    total_input_records: int,
    payload_accounts: list[dict[str, Any]],
    prepared_rows: list[dict[str, Any]],
    skipped_existing: list[dict[str, Any]],
    skipped_missing_email: list[dict[str, Any]],
    skipped_duplicate_input: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "generated_at": utc_now_iso(),
        "summary": {
            "files_scanned": len(files),
            "input_records": total_input_records,
            "remote_accounts": len(remote_accounts),
            "remote_emails": len(remote_emails),
            "accounts_to_import": len(payload_accounts),
            "skipped_existing": len(skipped_existing),
            "skipped_missing_email": len(skipped_missing_email),
            "skipped_duplicate_input": len(skipped_duplicate_input),
        },
        "settings": {
            "input_dir": str(settings["input_dir"]),
            "output_file": str(settings["output_file"]),
            "report_file": str(settings["report_file"]),
            "recursive": settings["recursive"],
            "include_pattern": settings["include_pattern"],
            "exclude_patterns": settings["exclude_patterns"],
            "platform": settings["platform"],
            "account_type": settings["account_type"],
            "concurrency": settings["concurrency"],
            "priority": settings["priority"],
            "name_source": settings["name_source"],
            "sub2api_base_url": settings.get("sub2api_base_url"),
            "sub2api_accounts_url": settings.get("sub2api_accounts_url"),
            "sub2api_page_size": settings["sub2api_page_size"],
            "sub2api_timezone": settings["sub2api_timezone"],
            "sub2api_platform_filter": settings["sub2api_platform_filter"],
            "sub2api_type_filter": settings["sub2api_type_filter"],
            "sub2api_status_filter": settings["sub2api_status_filter"],
            "sub2api_privacy_mode_filter": settings["sub2api_privacy_mode_filter"],
            "sub2api_group_filter": settings["sub2api_group_filter"],
            "sub2api_search_filter": settings["sub2api_search_filter"],
            "sub2api_lite": settings["sub2api_lite"],
            "skip_remote_dedupe": settings["skip_remote_dedupe"],
        },
        "prepared_accounts": prepared_rows,
        "skipped_existing": skipped_existing,
        "skipped_missing_email": skipped_missing_email,
        "skipped_duplicate_input": skipped_duplicate_input,
    }


def export_json(path: Path, rows: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a deduplicated sub2api import payload from local auth JSON files."
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG, help=f"Optional config file (default: {DEFAULT_CONFIG})")
    parser.add_argument("-i", "--input", help=f"Input directory (default: {DEFAULT_INPUT_DIR})")
    parser.add_argument(
        "-o",
        "--output",
        help=f"Output JSON file (default: {DEFAULT_OUTPUT_FILE})",
    )
    parser.add_argument(
        "--report-output",
        help=f"Deduplication report JSON file (default: {DEFAULT_REPORT_FILE})",
    )
    parser.add_argument(
        "--include",
        help=f"Input filename pattern (default: {DEFAULT_INCLUDE_PATTERN})",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=None,
        help="Exclude filename pattern (can be used multiple times)",
    )
    parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=None, help="Scan subdirectories recursively")
    parser.add_argument("--platform", help=f"Account platform (default: {DEFAULT_PLATFORM})")
    parser.add_argument("--account-type", help=f"Account type (default: {DEFAULT_ACCOUNT_TYPE})")
    parser.add_argument("--concurrency", type=int, help=f"Default account concurrency (default: {DEFAULT_CONCURRENCY})")
    parser.add_argument("--priority", type=int, help=f"Default account priority (default: {DEFAULT_PRIORITY})")
    parser.add_argument(
        "--name-source",
        choices=["email", "filename", "index"],
        help=f"How to generate account names (default: {DEFAULT_NAME_SOURCE})",
    )
    parser.add_argument("--name-prefix", help=f"Name prefix when --name-source=index (default: {DEFAULT_NAME_PREFIX})")
    parser.add_argument("--timeout", type=int, help=f"HTTP timeout in seconds (default: {DEFAULT_TIMEOUT})")
    parser.add_argument("--sub2api-base-url", help="sub2api root URL")
    parser.add_argument("--sub2api-accounts-url", help="sub2api accounts API URL override")
    parser.add_argument("--sub2api-token", help="sub2api Bearer token or raw token")
    parser.add_argument("--sub2api-timezone", help=f"Timezone query parameter (default: {DEFAULT_TIMEZONE})")
    parser.add_argument("--sub2api-page-size", type=int, help=f"Accounts page size (default: {DEFAULT_PAGE_SIZE})")
    parser.add_argument("--sub2api-platform-filter", help="Accounts API platform filter")
    parser.add_argument("--sub2api-type-filter", help="Accounts API type filter")
    parser.add_argument("--sub2api-status-filter", help="Accounts API status filter")
    parser.add_argument("--sub2api-privacy-mode-filter", help="Accounts API privacy_mode filter")
    parser.add_argument("--sub2api-group-filter", help="Accounts API group filter")
    parser.add_argument("--sub2api-search-filter", help="Accounts API search filter")
    parser.add_argument("--sub2api-lite", action=argparse.BooleanOptionalAction, default=None, help="Accounts API lite query flag")
    parser.add_argument("--skip-remote-dedupe", action="store_true", help="Do not call sub2api; build payload from local files only")
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, default=None, help="Enable debug logging")
    return parser


def build_settings(args: argparse.Namespace, root_conf: dict[str, Any]) -> dict[str, Any]:
    section = root_conf.get("transform") if isinstance(root_conf.get("transform"), dict) else {}
    exclude_patterns = args.exclude if args.exclude is not None else conf_get(
        section,
        "exclude",
        "exclude_patterns",
        default=["merged*.json", "import_payload*.json", "sub2api_accounts_import*.json", "sub2api_dedupe_report*.json"],
    )
    if isinstance(exclude_patterns, str):
        exclude_patterns = [exclude_patterns]
    if not isinstance(exclude_patterns, list) or not all(isinstance(item, str) for item in exclude_patterns):
        raise RuntimeError("exclude patterns must be a list of strings")

    settings = {
        "input_dir": Path(str(args.input or conf_get(section, "input_dir", default=DEFAULT_INPUT_DIR)).strip()).expanduser().resolve(),
        "output_file": Path(str(args.output or conf_get(section, "output", "output_file", default=DEFAULT_OUTPUT_FILE)).strip()).expanduser().resolve(),
        "report_file": Path(str(args.report_output or conf_get(section, "report_output", "report_file", default=DEFAULT_REPORT_FILE)).strip()).expanduser().resolve(),
        "include_pattern": str(args.include or conf_get(section, "include", "include_pattern", default=DEFAULT_INCLUDE_PATTERN)).strip(),
        "exclude_patterns": exclude_patterns,
        "recursive": bool(args.recursive) if args.recursive is not None else parse_bool(conf_get(section, "recursive", default=False)),
        "platform": str(args.platform or conf_get(section, "platform", default=DEFAULT_PLATFORM)).strip(),
        "account_type": str(args.account_type or conf_get(section, "account_type", default=DEFAULT_ACCOUNT_TYPE)).strip(),
        "concurrency": int(args.concurrency if args.concurrency is not None else conf_get(section, "concurrency", default=DEFAULT_CONCURRENCY)),
        "priority": int(args.priority if args.priority is not None else conf_get(section, "priority", default=DEFAULT_PRIORITY)),
        "name_source": str(args.name_source or conf_get(section, "name_source", default=DEFAULT_NAME_SOURCE)).strip(),
        "name_prefix": str(args.name_prefix or conf_get(section, "name_prefix", default=DEFAULT_NAME_PREFIX)).strip(),
        "timeout": int(args.timeout if args.timeout is not None else conf_get(section, "timeout", default=DEFAULT_TIMEOUT)),
        "sub2api_base_url": str(args.sub2api_base_url or conf_get(section, "sub2api_base_url", "base_url", default="")).strip(),
        "sub2api_accounts_url": str(args.sub2api_accounts_url or conf_get(section, "sub2api_accounts_url", "accounts_url", default="")).strip(),
        "sub2api_token": str(args.sub2api_token or conf_get(section, "sub2api_token", "token", default="")).strip(),
        "sub2api_timezone": str(args.sub2api_timezone or conf_get(section, "sub2api_timezone", "timezone", default=DEFAULT_TIMEZONE)).strip(),
        "sub2api_page_size": int(args.sub2api_page_size if args.sub2api_page_size is not None else conf_get(section, "sub2api_page_size", "page_size", default=DEFAULT_PAGE_SIZE)),
        "sub2api_platform_filter": str(args.sub2api_platform_filter or conf_get(section, "sub2api_platform_filter", "platform_filter", default="")).strip(),
        "sub2api_type_filter": str(args.sub2api_type_filter or conf_get(section, "sub2api_type_filter", "type_filter", default="")).strip(),
        "sub2api_status_filter": str(args.sub2api_status_filter or conf_get(section, "sub2api_status_filter", "status_filter", default="")).strip(),
        "sub2api_privacy_mode_filter": str(args.sub2api_privacy_mode_filter or conf_get(section, "sub2api_privacy_mode_filter", "privacy_mode_filter", default="")).strip(),
        "sub2api_group_filter": str(args.sub2api_group_filter or conf_get(section, "sub2api_group_filter", "group_filter", default="")).strip(),
        "sub2api_search_filter": str(args.sub2api_search_filter or conf_get(section, "sub2api_search_filter", "search_filter", default="")).strip(),
        "sub2api_lite": bool(args.sub2api_lite) if args.sub2api_lite is not None else parse_bool(conf_get(section, "sub2api_lite", "lite", default=True), default=True),
        "skip_remote_dedupe": bool(args.skip_remote_dedupe or parse_bool(conf_get(section, "skip_remote_dedupe", default=False), default=False)),
        "debug": bool(args.debug) if args.debug is not None else parse_bool(conf_get(section, "debug", default=False)),
    }

    if not settings["input_dir"].is_dir():
        raise RuntimeError(f"input directory does not exist: {settings['input_dir']}")
    if settings["concurrency"] < 0:
        raise RuntimeError("concurrency must be >= 0")
    if settings["priority"] < 0:
        raise RuntimeError("priority must be >= 0")
    if settings["timeout"] < 1:
        raise RuntimeError("timeout must be >= 1")
    if settings["sub2api_page_size"] < 1:
        raise RuntimeError("sub2api_page_size must be >= 1")
    if settings["name_source"] not in {"email", "filename", "index"}:
        raise RuntimeError("name_source must be one of: email, filename, index")

    if not settings["sub2api_accounts_url"] and not settings["sub2api_base_url"] and not settings["skip_remote_dedupe"]:
        LOGGER.info("sub2api remote dedupe disabled: no sub2api URL configured")
        settings["skip_remote_dedupe"] = True
    if not settings["skip_remote_dedupe"] and not settings["sub2api_token"]:
        raise RuntimeError("sub2api_token is required unless --skip-remote-dedupe is enabled")
    return settings


def main() -> int:
    args = build_parser().parse_args()
    root_conf = load_config(args.config)
    settings = build_settings(args, root_conf)
    configure_logging(settings["debug"])

    LOGGER.info("start: input_dir=%s output_file=%s", settings["input_dir"], settings["output_file"])

    files = collect_json_files(
        input_dir=settings["input_dir"],
        output_file=settings["output_file"],
        report_file=settings["report_file"],
        recursive=settings["recursive"],
        include_pattern=settings["include_pattern"],
        exclude_patterns=settings["exclude_patterns"],
    )
    if not files:
        raise RuntimeError("no matching JSON files found")
    LOGGER.info("matched local files: %s", len(files))

    remote_accounts: list[dict[str, Any]] = []
    remote_emails: set[str] = set()
    if settings["skip_remote_dedupe"]:
        LOGGER.info("skip remote dedupe: enabled")
    else:
        remote_accounts, remote_emails = fetch_existing_remote_emails(settings)

    total_input_records = 0
    import_accounts: list[dict[str, Any]] = []
    prepared_rows: list[dict[str, Any]] = []
    skipped_existing: list[dict[str, Any]] = []
    skipped_missing_email: list[dict[str, Any]] = []
    skipped_duplicate_input: list[dict[str, Any]] = []
    used_names: dict[str, int] = {}
    seen_input_emails: set[str] = set()
    counter = 1

    for path in files:
        raw = json.loads(path.read_text(encoding="utf-8"))
        for item_index, item in enumerate(normalize_records(raw), start=1):
            total_input_records += 1
            credentials = item if isinstance(item, dict) else {"raw_value": item}
            email = extract_local_email(credentials, path)
            row_meta = {
                "file": str(path),
                "record_index": item_index,
                "email": email or None,
            }

            if not email:
                skipped_missing_email.append({**row_meta, "reason": "missing_email"})
                continue
            if email in seen_input_emails:
                skipped_duplicate_input.append({**row_meta, "reason": "duplicate_email_in_input"})
                continue
            seen_input_emails.add(email)
            if email in remote_emails:
                skipped_existing.append({**row_meta, "reason": "already_exists_in_sub2api"})
                continue

            account = build_account_entry(credentials, path, counter, used_names, settings, email)
            import_accounts.append(account)
            prepared_rows.append(
                {
                    **row_meta,
                    "name": account["name"],
                    "platform": account["platform"],
                    "type": account["type"],
                }
            )
            counter += 1

    payload = build_import_payload(import_accounts)
    report = build_report(
        settings=settings,
        files=files,
        remote_accounts=remote_accounts,
        remote_emails=remote_emails,
        total_input_records=total_input_records,
        payload_accounts=import_accounts,
        prepared_rows=prepared_rows,
        skipped_existing=skipped_existing,
        skipped_missing_email=skipped_missing_email,
        skipped_duplicate_input=skipped_duplicate_input,
    )

    export_json(settings["output_file"], payload)
    export_json(settings["report_file"], report)

    LOGGER.info("input records: %s", total_input_records)
    LOGGER.info("accounts to import: %s", len(import_accounts))
    LOGGER.info("skipped existing: %s", len(skipped_existing))
    LOGGER.info("skipped missing email: %s", len(skipped_missing_email))
    LOGGER.info("skipped duplicate input: %s", len(skipped_duplicate_input))
    print(f"Built sub2api payload with {len(import_accounts)} accounts -> {settings['output_file']}")
    print(f"Wrote dedupe report -> {settings['report_file']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)
