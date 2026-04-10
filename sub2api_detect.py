#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from delete import (
    DEFAULT_CONFIG,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
    DEFAULT_WORKERS,
    compact_text,
    conf_get,
    configure_logging,
    load_config,
    maybe_json_loads,
    parse_bool,
    request_text,
    utc_now_iso,
)


DEFAULT_OUTPUT_DIR = "sub2api_detect_output"
DEFAULT_ACCOUNTS_PATH = "/api/v1/admin/accounts/"
DEFAULT_SUB2API_PAGE_SIZE = 100

LOGGER = logging.getLogger("sub2api_detect")


def sub2api_headers(x_api_key: str) -> dict[str, str]:
    key = str(x_api_key or "").strip()
    if not key:
        raise RuntimeError("missing sub2api x-api-key")
    return {
        "x-api-key": key,
        "Accept": "application/json, text/plain, */*",
    }


def normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def nested_dicts(item: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    queue: list[dict[str, Any]] = []

    def append_mapping(value: Any) -> None:
        if isinstance(value, dict):
            if value not in result:
                result.append(value)
                queue.append(value)
        elif isinstance(value, str):
            parsed = maybe_json_loads(value)
            if isinstance(parsed, dict) and parsed not in result:
                result.append(parsed)
                queue.append(parsed)

    append_mapping(item)
    while queue:
        source = queue.pop(0)
        for key in ("credentials", "credential", "profile", "auth", "metadata", "id_token"):
            append_mapping(source.get(key))
    return result


def extract_email(item: dict[str, Any]) -> str:
    for source in nested_dicts(item):
        for key in ("email", "account", "mail", "username", "login", "identifier", "name"):
            value = normalize_email(source.get(key))
            if value and "@" in value:
                return value
    return ""


def looks_like_account_item(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    direct_keys = {"email", "account", "name", "platform", "type", "status", "credentials", "id"}
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
    parsed = urlsplit(base_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page)
    query["page_size"] = str(DEFAULT_SUB2API_PAGE_SIZE)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


def build_test_url(settings: dict[str, Any], account_id: Any) -> str:
    base_url = build_accounts_url(settings, 1).split("?", 1)[0].rstrip("/")
    return f"{base_url}/{quote(str(account_id), safe='')}/test"


def fetch_sub2api_accounts(settings: dict[str, Any]) -> list[dict[str, Any]]:
    all_accounts: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    page = 1
    total_pages: int | None = None

    while True:
        url = build_accounts_url(settings, page)
        status, text = request_text(
            url,
            method="GET",
            headers=sub2api_headers(settings["sub2api_x_api_key"]),
            timeout=settings["timeout"],
        )
        if status >= 400:
            raise RuntimeError(f"sub2api accounts request failed: http {status} | {compact_text(text, 300) or '-'}")

        payload = maybe_json_loads(text)
        if not isinstance(payload, (dict, list)):
            raise RuntimeError("sub2api accounts response is not valid JSON")

        page_accounts = find_account_list(payload) or []
        for item in page_accounts:
            account_key = str(item.get("id") or item.get("name") or len(all_accounts)).strip()
            if account_key in seen_ids:
                continue
            seen_ids.add(account_key)
            all_accounts.append(item)

        if total_pages is None:
            total_pages = find_pagination_value(payload, ("pages", "page_count", "total_pages", "last_page"))
        LOGGER.info("fetched sub2api accounts page %s/%s: %s", page, total_pages or "?", len(page_accounts))

        if not page_accounts:
            break
        if total_pages is not None and page >= total_pages:
            break
        if total_pages is None and len(page_accounts) < DEFAULT_SUB2API_PAGE_SIZE:
            break
        page += 1

    LOGGER.info("fetched sub2api accounts total: %s", len(all_accounts))
    return all_accounts


def build_record(item: dict[str, Any]) -> dict[str, Any]:
    email = extract_email(item)
    return {
        "id": item.get("id"),
        "name": str(item.get("name") or "").strip() or None,
        "account": email or str(item.get("name") or "").strip() or None,
        "email": email or None,
        "platform": str(item.get("platform") or "").strip() or None,
        "type": str(item.get("type") or "").strip() or None,
        "status": str(item.get("status") or "").strip() or None,
        "notes": compact_text(item.get("notes"), 1200),
        "disabled": int(bool(item.get("disabled"))),
        "unavailable": int(bool(item.get("unavailable"))),
        "api_http_status": None,
        "api_status_code": None,
        "test_message": None,
        "test_response": None,
        "probe_error_kind": None,
        "probe_error_text": None,
        "is_invalid_401": 0,
        "last_probed_at": None,
        "updated_at": utc_now_iso(),
    }


def iter_nodes(node: Any, depth: int = 0) -> list[Any]:
    if depth > 6:
        return []
    nodes = [node]
    if isinstance(node, dict):
        for value in node.values():
            nodes.extend(iter_nodes(value, depth + 1))
    elif isinstance(node, list):
        for value in node:
            nodes.extend(iter_nodes(value, depth + 1))
    return nodes


def extract_status_code_from_payload(payload: Any) -> int | None:
    preferred_keys = (
        "status_code",
        "statusCode",
        "http_status",
        "httpStatus",
        "upstream_status",
        "upstreamStatus",
        "response_status",
        "responseStatus",
    )
    soft_keys = ("status", "code")
    candidates: list[Any] = []

    if isinstance(payload, dict):
        for key in ("data", "result", "payload", "response", "test_result", "testResult"):
            value = payload.get(key)
            if value is not None:
                candidates.append(value)
        candidates.append(payload)
    else:
        candidates.append(payload)

    for candidate in candidates:
        for node in iter_nodes(candidate):
            if not isinstance(node, dict):
                continue
            for key in preferred_keys:
                value = node.get(key)
                if isinstance(value, bool):
                    continue
                try:
                    if value is not None:
                        return int(value)
                except (TypeError, ValueError):
                    pass

    for candidate in candidates:
        for node in iter_nodes(candidate):
            if not isinstance(node, dict):
                continue
            for key in soft_keys:
                value = node.get(key)
                if isinstance(value, bool):
                    continue
                try:
                    if value is not None:
                        code = int(value)
                        if code in {200, 400, 401, 403, 429, 500}:
                            return code
                    continue
                except (TypeError, ValueError):
                    pass
    return None


def payload_has_401_hint(payload: Any) -> bool:
    for node in iter_nodes(payload):
        if isinstance(node, str):
            text = node.strip().lower()
            if "401" in text or "unauthorized" in text or "invalid token" in text:
                return True
    return False


def summarize_test_payload(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in ("message", "detail", "error", "msg", "status_text"):
            text = compact_text(payload.get(key), 1200)
            if text:
                return text
    return compact_text(payload, 1200)


def probe_account(record: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    result = dict(record)
    result["last_probed_at"] = utc_now_iso()
    account_id = result.get("id")
    if account_id in (None, ""):
        result["probe_error_kind"] = "missing_account_id"
        result["probe_error_text"] = "missing account id"
        return result

    url = build_test_url(settings, account_id)
    headers = sub2api_headers(settings["sub2api_x_api_key"])
    for attempt in range(settings["retries"] + 1):
        try:
            status, text = request_text(
                url,
                method="POST",
                headers=headers,
                timeout=settings["timeout"],
            )
            result["api_http_status"] = status
            payload = maybe_json_loads(text)
            result["test_message"] = summarize_test_payload(payload if payload is not None else text)

            if status == 429 or status >= 500:
                result["probe_error_kind"] = "sub2api_http_429" if status == 429 else "sub2api_http_5xx"
                result["probe_error_text"] = f"sub2api test http {status}"
            elif status == 401:
                result["api_status_code"] = 401
                result["is_invalid_401"] = 1
                result["probe_error_kind"] = None
                result["probe_error_text"] = None
                result["test_response"] = {"http_status": status, "message": result["test_message"]}
                return result
            elif status >= 400:
                result["api_status_code"] = status
                result["probe_error_kind"] = "sub2api_http_4xx"
                result["probe_error_text"] = f"sub2api test http {status}"
                result["test_response"] = {"http_status": status, "message": result["test_message"]}
                return result
            else:
                parsed = payload if isinstance(payload, (dict, list)) else text
                derived_status = extract_status_code_from_payload(parsed)
                if derived_status is None and payload_has_401_hint(parsed):
                    derived_status = 401
                if derived_status is None:
                    derived_status = 200
                result["api_status_code"] = derived_status
                result["is_invalid_401"] = int(derived_status == 401)
                result["probe_error_kind"] = None
                result["probe_error_text"] = None
                result["test_response"] = (
                    parsed
                    if isinstance(parsed, dict)
                    else {"http_status": status, "message": compact_text(parsed, 1200)}
                )
                return result
        except Exception as exc:
            result["probe_error_kind"] = "other"
            result["probe_error_text"] = compact_text(exc, 240)
        if attempt >= settings["retries"]:
            return result
        time.sleep(min(3.0, 0.5 * (2 ** attempt)))
    return result


def probe_records(records: list[dict[str, Any]], settings: dict[str, Any]) -> list[dict[str, Any]]:
    if not records:
        return []
    results: list[dict[str, Any]] = []
    total = len(records)
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=settings["workers"]) as executor:
        futures = [executor.submit(probe_account, record, settings) for record in records]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            done += 1
            if done % 100 == 0 or done == total:
                LOGGER.info("probe progress: %s/%s", done, total)
    return sorted(results, key=lambda row: str(row.get("email") or row.get("name") or ""))


def export_json(path: Path, rows: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def export_text_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def public_record(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "account": row.get("account") or row.get("email") or "",
        "email": row.get("email") or "",
        "platform": row.get("platform"),
        "type": row.get("type"),
        "status": row.get("status"),
        "notes": row.get("notes"),
        "disabled": bool(row.get("disabled")),
        "unavailable": bool(row.get("unavailable")),
        "api_http_status": row.get("api_http_status"),
        "api_status_code": row.get("api_status_code"),
        "test_message": row.get("test_message"),
        "test_response": row.get("test_response"),
        "probe_error_kind": row.get("probe_error_kind"),
        "probe_error_text": row.get("probe_error_text"),
        "is_invalid_401": row.get("is_invalid_401"),
        "last_probed_at": row.get("last_probed_at"),
        "updated_at": row.get("updated_at"),
    }


def output_401_emails(invalid_records: list[dict[str, Any]], path: Path) -> list[str]:
    emails = sorted(
        {
            str(row.get("email") or row.get("account") or "").strip()
            for row in invalid_records
            if str(row.get("email") or row.get("account") or "").strip()
        }
    )
    export_text_lines(path, emails)
    print("INVALID_401_EMAILS")
    if emails:
        for email in emails:
            print(email)
    else:
        print("(empty)")
    return emails


def build_settings(conf: dict[str, Any]) -> dict[str, Any]:
    transform_section = conf.get("transform") if isinstance(conf.get("transform"), dict) else {}
    output_dir = Path(DEFAULT_OUTPUT_DIR).expanduser()
    settings = {
        "timeout": int(conf_get(conf, "timeout", default=DEFAULT_TIMEOUT)),
        "retries": int(conf_get(conf, "retries", default=DEFAULT_RETRIES)),
        "workers": int(conf_get(conf, "workers", "probe_workers", default=DEFAULT_WORKERS)),
        "debug": parse_bool(conf_get(conf, "debug", default=False)),
        "output_dir": output_dir,
        "records_output": output_dir / "probe_records.json",
        "invalid_output": output_dir / "401_accounts.json",
        "email_output": output_dir / "401_emails.txt",
        "sub2api_base_url": str(conf_get(transform_section, "base_url", "sub2api_base_url", default="")).strip(),
        "sub2api_accounts_url": str(conf_get(transform_section, "accounts_url", "sub2api_accounts_url", default="")).strip(),
        "sub2api_x_api_key": str(conf_get(transform_section, "x_api_key", "sub2api_x_api_key", "api_key", default="")).strip(),
    }
    if settings["workers"] < 1 or settings["timeout"] < 1 or settings["retries"] < 0:
        raise RuntimeError("workers/timeout/retries config is invalid")
    if not settings["sub2api_accounts_url"] and not settings["sub2api_base_url"]:
        raise RuntimeError("missing transform.base_url or transform.accounts_url in config.json")
    if not settings["sub2api_x_api_key"]:
        raise RuntimeError("missing transform.x_api_key in config.json")
    return settings


def main() -> int:
    conf = load_config(DEFAULT_CONFIG)
    settings = build_settings(conf)
    configure_logging(settings["debug"])
    LOGGER.info("start: output_dir=%s workers=%s", settings["output_dir"], settings["workers"])

    raw_accounts = fetch_sub2api_accounts(settings)
    LOGGER.info("fetched sub2api accounts: %s", len(raw_accounts))
    records = probe_records([build_record(item) for item in raw_accounts if isinstance(item, dict)], settings)

    probe_rows = [public_record(row) for row in records]
    invalid_rows = [row for row in probe_rows if row.get("is_invalid_401") == 1]
    emails = output_401_emails(invalid_rows, settings["email_output"])

    export_json(settings["records_output"], probe_rows)
    export_json(settings["invalid_output"], invalid_rows)

    LOGGER.info("401 accounts: %s", len(invalid_rows))
    LOGGER.info("401 emails: %s", len(emails))
    LOGGER.info("probe records: %s", len(probe_rows))
    print(f"Wrote probe records -> {settings['records_output']}")
    print(f"Wrote 401 accounts -> {settings['invalid_output']}")
    print(f"Wrote 401 emails -> {settings['email_output']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("用户中断", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"错误: {exc}", file=sys.stderr)
        raise SystemExit(1)
