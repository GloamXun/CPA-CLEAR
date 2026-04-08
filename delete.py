#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
import json
import logging
import math
import shutil
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CONFIG = "config.json"
DEFAULT_TARGET_TYPE = "codex"
DEFAULT_TIMEOUT = 15
DEFAULT_RETRIES = 3
DEFAULT_WORKERS = 30
DEFAULT_USER_AGENT = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
DEFAULT_MOVE_MODE = "all"
DEFAULT_QUOTA_DISABLE_THRESHOLD = 0.0
WHAM_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
SPARK_METERED_FEATURE = "codex_bengalfox"

LOGGER = logging.getLogger("delete")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_config(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"config file not found: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"配置文件顶层必须是对象: {p}")
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


def opt_flag(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value if value in {0, 1} else None


def opt_num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def opt_ratio(value: Any) -> float | None:
    value = opt_num(value)
    if value is None:
        return None
    return 0.0 if value < 0 else 1.0 if value > 1 else value


def extract_remaining_ratio(rate_limit: dict[str, Any] | None) -> float | None:
    if not isinstance(rate_limit, dict):
        return None
    total_keys = ("total", "limit", "max", "maximum", "quota", "request_limit", "requests_limit", "total_requests")
    remaining_keys = ("remaining", "remaining_requests", "requests_remaining", "available", "available_requests", "left")
    used_keys = ("used", "consumed", "used_requests", "requests_used", "spent")
    windows: list[dict[str, Any]] = [rate_limit]
    for key in ("primary_window", "window", "current_window"):
        value = rate_limit.get(key)
        if isinstance(value, dict):
            windows.append(value)
    for window in windows:
        total = next((opt_num(window.get(key)) for key in total_keys if opt_num(window.get(key)) is not None), None)
        if total is None or total <= 0:
            continue
        remaining = next((opt_num(window.get(key)) for key in remaining_keys if opt_num(window.get(key)) is not None), None)
        used = next((opt_num(window.get(key)) for key in used_keys if opt_num(window.get(key)) is not None), None)
        if remaining is None and used is None:
            continue
        return opt_ratio((remaining / total) if remaining is not None else ((total - used) / total))
    return None


def find_spark_rate_limit(body: dict[str, Any]) -> dict[str, Any] | None:
    items = body.get("additional_rate_limits")
    if not isinstance(items, list):
        return None
    candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("rate_limit"), dict):
            candidates.append((item, item["rate_limit"]))
    for item, rate_limit in candidates:
        if str(item.get("metered_feature") or "").strip().lower() == SPARK_METERED_FEATURE:
            return rate_limit
    for item, rate_limit in candidates:
        if "spark" in str(item.get("limit_name") or "").strip().lower():
            return rate_limit
    return None


def resolve_quota_signal(record: dict[str, Any]) -> tuple[int | None, int | None, str]:
    plan_type = str(record.get("usage_plan_type") or record.get("id_token_plan_type") or "").strip().lower()
    spark_limit = opt_flag(record.get("usage_spark_limit_reached"))
    spark_allowed = opt_flag(record.get("usage_spark_allowed"))
    primary_limit = opt_flag(record.get("usage_limit_reached"))
    primary_allowed = opt_flag(record.get("usage_allowed"))
    if plan_type == "pro" and spark_limit is not None:
        return spark_limit, spark_allowed if spark_allowed is not None else primary_allowed, "spark"
    return primary_limit, primary_allowed, "primary"


def resolve_quota_ratio(record: dict[str, Any]) -> tuple[float | None, str]:
    _, _, source = resolve_quota_signal(record)
    primary = opt_ratio(record.get("usage_remaining_ratio"))
    spark = opt_ratio(record.get("usage_spark_remaining_ratio"))
    if source == "spark":
        return (spark, "spark") if spark is not None else ((primary, "primary_fallback") if primary is not None else (None, "spark"))
    return (primary, "primary") if primary is not None else ((spark, "spark_fallback") if spark is not None else (None, "primary"))


def mgmt_headers(token: str, json_mode: bool = False) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json, text/plain, */*"}
    if json_mode:
        headers["Content-Type"] = "application/json"
    return headers


def request_text(url: str, *, method: str, headers: dict[str, str], timeout: int, payload: dict[str, Any] | None = None) -> tuple[int, str]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(url=url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as exc:
        raise RuntimeError(str(getattr(exc, "reason", exc))) from exc


def get_name(item: dict[str, Any]) -> str:
    return str(item.get("name") or item.get("id") or "").strip()


def get_type(item: dict[str, Any]) -> str:
    return str(item.get("type") or item.get("typo") or "").strip()


def get_account(item: dict[str, Any]) -> str:
    return str(item.get("account") or item.get("email") or "").strip()


def get_account_id(item: dict[str, Any]) -> str:
    id_token = maybe_json_loads(item.get("id_token"))
    id_token = id_token if isinstance(id_token, dict) else {}
    for source in (id_token, item):
        for key in ("chatgpt_account_id", "chatgptAccountId", "account_id", "accountId"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def get_plan_type(item: dict[str, Any]) -> str:
    id_token = maybe_json_loads(item.get("id_token"))
    if isinstance(id_token, dict) and isinstance(id_token.get("plan_type"), str):
        return id_token["plan_type"].strip()
    return ""


def match_filters(item: dict[str, Any], target_type: str, provider: str) -> bool:
    target_type = target_type.strip().lower()
    provider = provider.strip().lower()
    item_type = get_type(item).lower()
    item_provider = str(item.get("provider") or "").strip().lower()
    return (target_type in {"", "all", "*"} or item_type == target_type) and (provider in {"", "all", "*"} or item_provider == provider)


def build_record(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": get_name(item),
        "account": get_account(item) or None,
        "email": str(item.get("email") or "").strip() or None,
        "provider": str(item.get("provider") or "").strip() or None,
        "source": str(item.get("source") or "").strip() or None,
        "type": get_type(item) or None,
        "disabled": int(bool(item.get("disabled"))),
        "unavailable": int(bool(item.get("unavailable"))),
        "status": str(item.get("status") or "").strip() or None,
        "status_message": compact_text(item.get("status_message"), 1200),
        "auth_index": str(item.get("auth_index") or "").strip() or None,
        "chatgpt_account_id": get_account_id(item) or None,
        "id_token_plan_type": get_plan_type(item) or None,
        "api_http_status": None,
        "api_status_code": None,
        "usage_allowed": None,
        "usage_limit_reached": None,
        "usage_plan_type": None,
        "usage_email": None,
        "usage_reset_at": None,
        "usage_reset_after_seconds": None,
        "usage_spark_allowed": None,
        "usage_spark_limit_reached": None,
        "usage_spark_reset_at": None,
        "usage_spark_reset_after_seconds": None,
        "usage_remaining_ratio": None,
        "usage_spark_remaining_ratio": None,
        "probe_error_kind": None,
        "probe_error_text": None,
        "last_probed_at": None,
        "updated_at": utc_now_iso(),
    }


def fetch_auth_files(base_url: str, token: str, timeout: int) -> list[dict[str, Any]]:
    status, text = request_text(
        f"{base_url.rstrip('/')}/v0/management/auth-files",
        method="GET",
        headers=mgmt_headers(token),
        timeout=timeout,
    )
    if status >= 400:
        raise RuntimeError(f"拉取 auth-files 失败: http {status} | {compact_text(text, 300) or '-'}")
    data = maybe_json_loads(text)
    files = data.get("files") if isinstance(data, dict) else None
    return [row for row in files if isinstance(row, dict)] if isinstance(files, list) else []


def probe_account(record: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    result = dict(record)
    result["last_probed_at"] = utc_now_iso()
    auth_index = str(result.get("auth_index") or "").strip()
    account_id = str(result.get("chatgpt_account_id") or "").strip()
    if not auth_index:
        result["probe_error_kind"] = "missing_auth_index"
        result["probe_error_text"] = "missing auth_index"
        return result
    if not account_id:
        result["probe_error_kind"] = "missing_chatgpt_account_id"
        result["probe_error_text"] = "missing Chatgpt-Account-Id"
        return result
    payload = {
        "authIndex": auth_index,
        "method": "GET",
        "url": WHAM_USAGE_URL,
        "header": {
            "Authorization": "Bearer $TOKEN$",
            "Content-Type": "application/json",
            "User-Agent": settings["user_agent"],
            "Chatgpt-Account-Id": account_id,
        },
    }
    url = f"{settings['base_url'].rstrip('/')}/v0/management/api-call"
    for attempt in range(settings["retries"] + 1):
        try:
            status, text = request_text(url, method="POST", headers=mgmt_headers(settings["token"], True), timeout=settings["timeout"], payload=payload)
            result["api_http_status"] = status
            if status == 429 or status >= 500:
                result["probe_error_kind"] = "management_api_http_429" if status == 429 else "management_api_http_5xx"
                result["probe_error_text"] = f"management api-call http {status}"
            elif status >= 400:
                result["probe_error_kind"] = "management_api_http_4xx"
                result["probe_error_text"] = f"management api-call http {status}"
                return result
            else:
                outer = maybe_json_loads(text)
                if not isinstance(outer, dict):
                    result["probe_error_kind"] = "api_call_invalid_json"
                    result["probe_error_text"] = "api-call response is not valid JSON"
                    return result
                result["api_status_code"] = outer.get("status_code")
                if result["api_status_code"] is None:
                    result["probe_error_kind"] = "missing_status_code"
                    result["probe_error_text"] = "missing status_code in api-call response"
                    return result
                if result["api_status_code"] == 401:
                    result["probe_error_kind"] = None
                    result["probe_error_text"] = None
                    return result
                body = outer.get("body")
                parsed = body if isinstance(body, dict) else maybe_json_loads(body) if isinstance(body, str) else {} if body is None else None
                if parsed is None or (parsed and not isinstance(parsed, dict)):
                    result["probe_error_kind"] = "body_not_object"
                    result["probe_error_text"] = f"api-call body is not JSON object: {type(body).__name__}"
                    return result
                rate_limit = parsed.get("rate_limit") if isinstance(parsed, dict) else None
                primary_window = rate_limit.get("primary_window") if isinstance(rate_limit, dict) else None
                spark = find_spark_rate_limit(parsed) if isinstance(parsed, dict) else None
                spark_window = spark.get("primary_window") if isinstance(spark, dict) else None
                result["usage_allowed"] = int(rate_limit.get("allowed")) if isinstance(rate_limit, dict) and isinstance(rate_limit.get("allowed"), bool) else None
                result["usage_limit_reached"] = int(rate_limit.get("limit_reached")) if isinstance(rate_limit, dict) and isinstance(rate_limit.get("limit_reached"), bool) else None
                result["usage_remaining_ratio"] = extract_remaining_ratio(rate_limit)
                result["usage_plan_type"] = str(parsed.get("plan_type") or "").strip() or None
                result["usage_email"] = str(parsed.get("email") or "").strip() or None
                result["usage_reset_at"] = int(primary_window.get("reset_at")) if isinstance(primary_window, dict) and primary_window.get("reset_at") is not None else None
                result["usage_reset_after_seconds"] = int(primary_window.get("reset_after_seconds")) if isinstance(primary_window, dict) and primary_window.get("reset_after_seconds") is not None else None
                result["usage_spark_allowed"] = int(spark.get("allowed")) if isinstance(spark, dict) and isinstance(spark.get("allowed"), bool) else None
                result["usage_spark_limit_reached"] = int(spark.get("limit_reached")) if isinstance(spark, dict) and isinstance(spark.get("limit_reached"), bool) else None
                result["usage_spark_remaining_ratio"] = extract_remaining_ratio(spark)
                result["usage_spark_reset_at"] = int(spark_window.get("reset_at")) if isinstance(spark_window, dict) and spark_window.get("reset_at") is not None else None
                result["usage_spark_reset_after_seconds"] = int(spark_window.get("reset_after_seconds")) if isinstance(spark_window, dict) and spark_window.get("reset_after_seconds") is not None else None
                result["probe_error_kind"] = None if result["api_status_code"] == 200 else "other"
                result["probe_error_text"] = None if result["api_status_code"] == 200 else f"unexpected upstream status_code={result['api_status_code']}"
                return result
        except Exception as exc:
            result["probe_error_kind"] = "other"
            result["probe_error_text"] = compact_text(exc, 240)
        if attempt >= settings["retries"]:
            return result
        time.sleep(min(3.0, 0.5 * (2 ** attempt)))
    return result


def classify(record: dict[str, Any], threshold: float) -> dict[str, Any]:
    result = dict(record)
    invalid_401 = bool(result.get("unavailable")) or result.get("api_status_code") == 401
    limit_reached, allowed, signal_source = resolve_quota_signal(result)
    remaining_ratio, ratio_source = resolve_quota_ratio(result)
    threshold_hit = threshold > 0 and remaining_ratio is not None and remaining_ratio <= threshold
    quota_limited = not invalid_401 and result.get("api_status_code") == 200 and (limit_reached == 1 or threshold_hit)
    result["quota_signal_source"] = signal_source
    result["quota_remaining_ratio"] = remaining_ratio
    result["quota_remaining_ratio_source"] = ratio_source
    result["quota_threshold_triggered"] = int(threshold_hit)
    result["effective_allowed"] = allowed
    result["is_invalid_401"] = int(invalid_401)
    result["is_quota_limited"] = int(quota_limited)
    result["updated_at"] = utc_now_iso()
    return result


def discover_input_files(input_dir: Path, recursive: bool, output_dir: Path) -> list[Path]:
    if not input_dir.exists():
        raise RuntimeError(f"input_dir 不存在: {input_dir}")
    if not input_dir.is_dir():
        raise RuntimeError(f"input_dir 不是目录: {input_dir}")
    output_root = output_dir.resolve()
    iterator = input_dir.rglob("*.json") if recursive else input_dir.glob("*.json")
    files: list[Path] = []
    for path in iterator:
        if not path.is_file():
            continue
        try:
            if path.resolve().is_relative_to(output_root):
                continue
        except ValueError:
            pass
        files.append(path)
    return sorted(files, key=lambda item: str(item))


def build_move_target(source: Path, input_dir: Path, output_dir: Path) -> Path:
    target = output_dir / source.relative_to(input_dir)
    if not target.exists():
        return target
    index = 1
    while True:
        candidate = target.with_name(f"{target.stem}__{index}{target.suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def move_detected_files(invalid_records: list[dict[str, Any]], quota_records: list[dict[str, Any]], settings: dict[str, Any]) -> list[dict[str, Any]]:
    files = discover_input_files(settings["input_dir"], settings["recursive"], settings["output_dir"])
    exact: dict[str, list[Path]] = {}
    stem: dict[str, list[Path]] = {}
    for path in files:
        exact.setdefault(path.name, []).append(path)
        stem.setdefault(path.stem, []).append(path)
    selected: list[tuple[str, dict[str, Any]]] = []
    if settings["move_mode"] in {"all", "401"}:
        selected.extend(("401", row) for row in invalid_records)
    if settings["move_mode"] in {"all", "quota"}:
        selected.extend(("quota", row) for row in quota_records)
    moved: set[str] = set()
    results: list[dict[str, Any]] = []
    for category, row in selected:
        name = str(row.get("name") or "").strip()
        if settings["debug"] and category == "401":
            results.append(
                {
                    "category": category,
                    "name": name,
                    "ok": True,
                    "source": None,
                    "target": None,
                    "skipped": True,
                    "reason": "debug_skip_401_move",
                }
            )
            continue
        matches = list(exact.get(name, [])) or list(exact.get(f"{name}.json", [])) or list(stem.get(Path(name).stem, []))
        if not matches:
            results.append({"category": category, "name": name, "ok": False, "source": None, "target": None, "error": "local file not found in input_dir"})
            continue
        for source in matches:
            source_key = str(source.resolve())
            if source_key in moved:
                continue
            target = build_move_target(source, settings["input_dir"], settings["output_dir"])
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(target))
            moved.add(source_key)
            results.append({"category": category, "name": name, "ok": True, "source": str(source), "target": str(target)})
    return results


def export_json(path: Path, rows: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def export_text_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def probe_records(records: list[dict[str, Any]], settings: dict[str, Any]) -> list[dict[str, Any]]:
    if not records:
        return []
    results: list[dict[str, Any]] = []
    total = len(records)
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=settings["workers"]) as executor:
        futures = [executor.submit(probe_account, record, settings) for record in records]
        for future in concurrent.futures.as_completed(futures):
            results.append(classify(future.result(), settings["quota_disable_threshold"]))
            done += 1
            if done % 100 == 0 or done == total:
                LOGGER.info("探测进度: %s/%s", done, total)
    return sorted(results, key=lambda row: str(row.get("name") or ""))


def legacy_build_settings(args, conf: dict[str, Any]) -> dict[str, Any]:
    output_dir = Path(str(args.output_dir or conf_get(conf, "output_dir", default="output_dir")).strip()).expanduser()
    settings = {
        "base_url": str(args.base_url or conf_get(conf, "base_url", default="")).strip(),
        "token": str(args.token or conf_get(conf, "token", default="")).strip(),
        "target_type": str(args.target_type if args.target_type is not None else conf_get(conf, "target_type", default=DEFAULT_TARGET_TYPE)).strip(),
        "provider": str(args.provider if args.provider is not None else conf_get(conf, "provider", default="")).strip(),
        "timeout": int(args.timeout if args.timeout is not None else conf_get(conf, "timeout", default=DEFAULT_TIMEOUT)),
        "retries": int(args.retries if args.retries is not None else conf_get(conf, "retries", default=DEFAULT_RETRIES)),
        "workers": int(args.workers if args.workers is not None else conf_get(conf, "workers", "probe_workers", default=DEFAULT_WORKERS)),
        "user_agent": str(args.user_agent if args.user_agent is not None else conf_get(conf, "user_agent", default=DEFAULT_USER_AGENT)).strip(),
        "quota_disable_threshold": float(args.quota_disable_threshold if args.quota_disable_threshold is not None else conf_get(conf, "quota_disable_threshold", default=0.0)),
        "input_dir": Path(str(args.input_dir or conf_get(conf, "input_dir", "upload_dir", default="")).strip()).expanduser(),
        "output_dir": output_dir,
        "recursive": bool(args.recursive) if args.recursive is not None else parse_bool(conf_get(conf, "recursive", "upload_recursive", default=False)),
        "move_mode": str(args.move_mode or conf_get(conf, "move_mode", default=DEFAULT_MOVE_MODE)).strip().lower(),
        "invalid_output": Path(str(args.invalid_output or conf_get(conf, "invalid_output", default=output_dir / "401_accounts.json"))).expanduser(),
        "quota_output": Path(str(args.quota_output or conf_get(conf, "quota_output", default=output_dir / "quota_accounts.json"))).expanduser(),
        "move_output": Path(str(args.move_output or conf_get(conf, "move_output", default=output_dir / "move_results.json"))).expanduser(),
        "dry_run": bool(args.dry_run),
    }
    if not settings["base_url"] or not settings["token"] or not str(settings["input_dir"]):
        raise RuntimeError("必须提供 base_url、token、input_dir")
    if settings["workers"] < 1 or settings["timeout"] < 1 or settings["retries"] < 0:
        raise RuntimeError("workers/timeout/retries 参数不合法")
    if settings["move_mode"] not in {"all", "401", "quota"}:
        raise RuntimeError("move_mode 只能是 all / 401 / quota")
    return settings


def legacy_build_parser():
    parser = argparse.ArgumentParser(description="使用 CPA 接口识别 401/quota 账号，并将本地账号文件移动到 output_dir")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--base-url")
    parser.add_argument("--token")
    parser.add_argument("--input-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--target-type")
    parser.add_argument("--provider")
    parser.add_argument("--timeout", type=int)
    parser.add_argument("--retries", type=int)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--user-agent")
    parser.add_argument("--quota-disable-threshold", type=float)
    parser.add_argument("--move-mode", choices=["all", "401", "quota"])
    parser.add_argument("--invalid-output")
    parser.add_argument("--quota-output")
    parser.add_argument("--move-output")
    parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser


def legacy_main() -> int:
    args = legacy_build_parser().parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    settings = legacy_build_settings(args, load_config(args.config))
    LOGGER.info("开始执行: input_dir=%s output_dir=%s", settings["input_dir"], settings["output_dir"])
    files = fetch_auth_files(settings["base_url"], settings["token"], settings["timeout"])
    filtered = [item for item in files if match_filters(item, settings["target_type"], settings["provider"]) and get_name(item)]
    LOGGER.info("过滤后的远端账号数: %s", len(filtered))
    records = probe_records([build_record(item) for item in filtered], settings)
    invalid_records = [
        {
            "name": row.get("name"),
            "account": row.get("account") or row.get("email") or "",
            "email": row.get("email") or "",
            "provider": row.get("provider"),
            "source": row.get("source"),
            "disabled": bool(row.get("disabled")),
            "unavailable": bool(row.get("unavailable")),
            "auth_index": row.get("auth_index"),
            "chatgpt_account_id": row.get("chatgpt_account_id"),
            "api_http_status": row.get("api_http_status"),
            "api_status_code": row.get("api_status_code"),
            "status": row.get("status"),
            "status_message": row.get("status_message"),
            "probe_error_kind": row.get("probe_error_kind"),
            "probe_error_text": row.get("probe_error_text"),
        }
        for row in records if row.get("is_invalid_401") == 1
    ]
    quota_records = [
        {
            "name": row.get("name"),
            "account": row.get("account") or row.get("email") or "",
            "email": row.get("usage_email") or row.get("email") or "",
            "provider": row.get("provider"),
            "source": row.get("source"),
            "disabled": bool(row.get("disabled")),
            "unavailable": bool(row.get("unavailable")),
            "auth_index": row.get("auth_index"),
            "chatgpt_account_id": row.get("chatgpt_account_id"),
            "api_http_status": row.get("api_http_status"),
            "api_status_code": row.get("api_status_code"),
            "limit_reached": bool(resolve_quota_signal(row)[0]) if resolve_quota_signal(row)[0] is not None else None,
            "allowed": bool(resolve_quota_signal(row)[1]) if resolve_quota_signal(row)[1] is not None else None,
            "quota_signal_source": row.get("quota_signal_source"),
            "remaining_ratio": row.get("quota_remaining_ratio"),
            "remaining_ratio_source": row.get("quota_remaining_ratio_source"),
            "threshold_triggered": bool(row.get("quota_threshold_triggered")),
            "primary_remaining_ratio": opt_ratio(row.get("usage_remaining_ratio")),
            "spark_remaining_ratio": opt_ratio(row.get("usage_spark_remaining_ratio")),
            "plan_type": row.get("usage_plan_type") or row.get("id_token_plan_type"),
            "probe_error_kind": row.get("probe_error_kind"),
            "probe_error_text": row.get("probe_error_text"),
        }
        for row in records if row.get("is_quota_limited") == 1
    ]
    settings["output_dir"].mkdir(parents=True, exist_ok=True)
    export_json(settings["invalid_output"], invalid_records)
    export_json(settings["quota_output"], quota_records)
    move_results = move_detected_files(invalid_records, quota_records, settings)
    export_json(settings["move_output"], move_results)
    LOGGER.info("401 账号数: %s", len(invalid_records))
    LOGGER.info("quota 账号数: %s", len(quota_records))
    LOGGER.info("移动成功: %s", sum(1 for row in move_results if row.get("ok")))
    LOGGER.info("移动失败: %s", sum(1 for row in move_results if not row.get("ok")))
    return 0

def configure_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def output_debug_401_emails(invalid_records: list[dict[str, Any]], settings: dict[str, Any]) -> list[str]:
    emails = sorted(
        {
            str(row.get("email") or row.get("account") or "").strip()
            for row in invalid_records
            if str(row.get("email") or row.get("account") or "").strip()
        }
    )
    export_text_lines(settings["debug_email_output"], emails)
    print("DEBUG_401_EMAILS")
    if emails:
        for email in emails:
            print(email)
    else:
        print("(empty)")
    return emails


def build_settings(conf: dict[str, Any]) -> dict[str, Any]:
    output_dir = Path(str(conf_get(conf, "output_dir", default="output_dir")).strip()).expanduser()
    settings = {
        "base_url": str(conf_get(conf, "base_url", default="")).strip(),
        "token": str(conf_get(conf, "token", default="")).strip(),
        "target_type": str(conf_get(conf, "target_type", default=DEFAULT_TARGET_TYPE)).strip(),
        "provider": str(conf_get(conf, "provider", default="")).strip(),
        "timeout": int(conf_get(conf, "timeout", default=DEFAULT_TIMEOUT)),
        "retries": int(conf_get(conf, "retries", default=DEFAULT_RETRIES)),
        "workers": int(conf_get(conf, "workers", "probe_workers", default=DEFAULT_WORKERS)),
        "user_agent": str(conf_get(conf, "user_agent", default=DEFAULT_USER_AGENT)).strip(),
        "quota_disable_threshold": float(conf_get(conf, "quota_disable_threshold", default=DEFAULT_QUOTA_DISABLE_THRESHOLD)),
        "input_dir": Path(str(conf_get(conf, "input_dir", "upload_dir", default="")).strip()).expanduser(),
        "output_dir": output_dir,
        "recursive": parse_bool(conf_get(conf, "recursive", "upload_recursive", default=False)),
        "debug": parse_bool(conf_get(conf, "debug", default=False)),
        "move_mode": str(conf_get(conf, "move_mode", default=DEFAULT_MOVE_MODE)).strip().lower(),
        "invalid_output": Path(str(conf_get(conf, "invalid_output", default=output_dir / "401_accounts.json"))).expanduser(),
        "quota_output": Path(str(conf_get(conf, "quota_output", default=output_dir / "quota_accounts.json"))).expanduser(),
        "move_output": Path(str(conf_get(conf, "move_output", default=output_dir / "move_results.json"))).expanduser(),
        "debug_email_output": Path(str(conf_get(conf, "debug_email_output", default=output_dir / "401_emails.txt"))).expanduser(),
    }
    if not settings["base_url"] or not settings["token"] or not str(settings["input_dir"]):
        raise RuntimeError("missing base_url, token or input_dir in config.json")
    if settings["workers"] < 1 or settings["timeout"] < 1 or settings["retries"] < 0:
        raise RuntimeError("workers/timeout/retries config is invalid")
    if settings["move_mode"] not in {"all", "401", "quota"}:
        raise RuntimeError("move_mode must be one of: all, 401, quota")
    if settings["quota_disable_threshold"] < 0:
        raise RuntimeError("quota_disable_threshold must be >= 0")
    return settings


def main() -> int:
    conf = load_config(DEFAULT_CONFIG)
    configure_logging(parse_bool(conf_get(conf, "debug", default=False)))
    settings = build_settings(conf)
    LOGGER.info(
        "start: input_dir=%s output_dir=%s move_mode=%s debug=%s",
        settings["input_dir"],
        settings["output_dir"],
        settings["move_mode"],
        settings["debug"],
    )
    files = fetch_auth_files(settings["base_url"], settings["token"], settings["timeout"])
    filtered = [item for item in files if match_filters(item, settings["target_type"], settings["provider"]) and get_name(item)]
    LOGGER.info("filtered remote accounts: %s", len(filtered))
    records = probe_records([build_record(item) for item in filtered], settings)
    invalid_records = [
        {
            "name": row.get("name"),
            "account": row.get("account") or row.get("email") or "",
            "email": row.get("usage_email") or row.get("email") or "",
            "provider": row.get("provider"),
            "source": row.get("source"),
            "disabled": bool(row.get("disabled")),
            "unavailable": bool(row.get("unavailable")),
            "auth_index": row.get("auth_index"),
            "chatgpt_account_id": row.get("chatgpt_account_id"),
            "api_http_status": row.get("api_http_status"),
            "api_status_code": row.get("api_status_code"),
            "status": row.get("status"),
            "status_message": row.get("status_message"),
            "probe_error_kind": row.get("probe_error_kind"),
            "probe_error_text": row.get("probe_error_text"),
        }
        for row in records if row.get("is_invalid_401") == 1
    ]
    quota_records = [
        {
            "name": row.get("name"),
            "account": row.get("account") or row.get("email") or "",
            "email": row.get("usage_email") or row.get("email") or "",
            "provider": row.get("provider"),
            "source": row.get("source"),
            "disabled": bool(row.get("disabled")),
            "unavailable": bool(row.get("unavailable")),
            "auth_index": row.get("auth_index"),
            "chatgpt_account_id": row.get("chatgpt_account_id"),
            "api_http_status": row.get("api_http_status"),
            "api_status_code": row.get("api_status_code"),
            "limit_reached": bool(resolve_quota_signal(row)[0]) if resolve_quota_signal(row)[0] is not None else None,
            "allowed": bool(resolve_quota_signal(row)[1]) if resolve_quota_signal(row)[1] is not None else None,
            "quota_signal_source": row.get("quota_signal_source"),
            "remaining_ratio": row.get("quota_remaining_ratio"),
            "remaining_ratio_source": row.get("quota_remaining_ratio_source"),
            "threshold_triggered": bool(row.get("quota_threshold_triggered")),
            "primary_remaining_ratio": opt_ratio(row.get("usage_remaining_ratio")),
            "spark_remaining_ratio": opt_ratio(row.get("usage_spark_remaining_ratio")),
            "plan_type": row.get("usage_plan_type") or row.get("id_token_plan_type"),
            "probe_error_kind": row.get("probe_error_kind"),
            "probe_error_text": row.get("probe_error_text"),
        }
        for row in records if row.get("is_quota_limited") == 1
    ]
    settings["output_dir"].mkdir(parents=True, exist_ok=True)
    export_json(settings["invalid_output"], invalid_records)
    export_json(settings["quota_output"], quota_records)
    if settings["debug"]:
        debug_emails = output_debug_401_emails(invalid_records, settings)
        LOGGER.info("debug 401 emails: %s", len(debug_emails))
    move_results = move_detected_files(invalid_records, quota_records, settings)
    export_json(settings["move_output"], move_results)
    LOGGER.info("401 accounts: %s", len(invalid_records))
    LOGGER.info("quota accounts: %s", len(quota_records))
    LOGGER.info("move success: %s", sum(1 for row in move_results if row.get("ok") and not row.get("skipped")))
    LOGGER.info("move failed: %s", sum(1 for row in move_results if not row.get("ok")))
    LOGGER.info("move skipped: %s", sum(1 for row in move_results if row.get("skipped")))
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
