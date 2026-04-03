#!/usr/bin/env python3
from __future__ import annotations

import argparse
import errno
import json
import ntpath
import os
import random
import re
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional
from urllib.parse import unquote, urlparse

CHUNK_SIZE = 1024 * 1024
DEFAULT_RESERVE_MB = 256
INTERNAL_STORAGE_PREFIXES = (
    "/storage/emulated",
    "/storage/self",
    "/sdcard",
    "/mnt/shell/emulated",
)
DEFAULT_CONFIG_FILENAMES = ("config.yaml", "config.yml")
SAMPLE_CONFIG_FILENAMES = ("config.sample.yaml", "config.sample.yml")
PROTECTED_DEDUPE_FILENAMES = {
    ".nomedia",
    "thumbs.db",
    "desktop.ini",
    ".ds_store",
}


@dataclass(frozen=True)
class SourceSpec:
    root: str
    alias: str
    kind: str  # local or smb


@dataclass(frozen=True)
class TargetSpec:
    path: str
    reserve_mb: Optional[int] = None  # If None, uses global reserve_mb


@dataclass(frozen=True)
class SourceFile:
    source_alias: str
    source_root: str
    source_path: str
    relative_path: str
    size: int
    mtime: Optional[float]
    kind: str


@dataclass
class TargetState:
    root: Path
    free_bytes: int
    reserve_bytes: int
    usable_bytes: int
    remaining_bytes: int
    planned_bytes: int = 0
    planned_files: int = 0


@dataclass(frozen=True)
class PlannedCopy:
    entry: SourceFile
    target_root: Path
    destination_path: Path


FileSignature = tuple[str, int]


@dataclass(frozen=True)
class DestinationFile:
    target_root: Path
    path: Path
    relative_path: str
    size: int


def format_bytes(size: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{size} B"


def is_smb_source(path: str) -> bool:
    lowered = path.lower()
    return path.startswith("\\\\") or path.startswith("//") or lowered.startswith("smb://")


def smb_url_to_unc(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme.lower() != "smb":
        raise ValueError(f"Unsupported SMB URL: {url}")
    if not parsed.hostname:
        raise ValueError(f"SMB URL is missing hostname: {url}")
    share_and_path = unquote(parsed.path).lstrip("/")
    if not share_and_path:
        raise ValueError(f"SMB URL is missing share/path: {url}")
    return "\\\\" + parsed.hostname + "\\" + share_and_path.replace("/", "\\")


def sanitize_alias(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    cleaned = cleaned.strip("._")
    return cleaned or "source"


def make_source_alias(source: str, index: int, used_aliases: set[str]) -> str:
    trimmed = source.rstrip("\\/")
    parts = [part for part in re.split(r"[\\/]+", trimmed) if part]
    base = parts[-1] if parts else f"source_{index}"
    alias = sanitize_alias(base)
    if alias in used_aliases:
        suffix = 2
        while f"{alias}_{suffix}" in used_aliases:
            suffix += 1
        alias = f"{alias}_{suffix}"
    used_aliases.add(alias)
    return alias


def build_source_specs(raw_sources: list[str]) -> list[SourceSpec]:
    specs: list[SourceSpec] = []
    used_aliases: set[str] = set()

    for index, raw_source in enumerate(raw_sources, start=1):
        source = raw_source.strip()
        if not source:
            continue

        if source.lower().startswith("smb://"):
            source = smb_url_to_unc(source)

        kind = "smb" if is_smb_source(source) else "local"
        if kind == "local":
            source = str(Path(os.path.expanduser(source)).resolve())
            if not os.path.isdir(source):
                raise RuntimeError(f"Source folder not found: {source}")

        alias = make_source_alias(source, index, used_aliases)
        specs.append(SourceSpec(root=source, alias=alias, kind=kind))

    if not specs:
        raise RuntimeError("At least one source folder is required.")

    return specs


def import_smbclient():
    try:
        import smbclient  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "SMB source detected, but smbprotocol is not installed. Install with: pip install smbprotocol"
        ) from exc
    return smbclient


def configure_smb_client(
    smbclient,
    username: Optional[str],
    password: Optional[str],
    domain: Optional[str],
) -> None:
    username = (username or "").strip() or os.getenv("SMB_USERNAME")
    password = (password or "").strip() or os.getenv("SMB_PASSWORD")
    domain = (domain or "").strip() or os.getenv("SMB_DOMAIN")

    config: dict[str, str] = {}
    if username:
        config["username"] = username
    if password:
        config["password"] = password
    if domain:
        config["domain"] = domain

    if config:
        smbclient.ClientConfig(**config)


def import_yaml():
    try:
        import yaml  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Config file support requires PyYAML. Install with: pip install pyyaml"
        ) from exc
    return yaml


def discover_default_config_path() -> Optional[str]:
    for candidate in DEFAULT_CONFIG_FILENAMES:
        if Path(candidate).is_file():
            return candidate
    return None


def discover_sample_config_path() -> Optional[str]:
    for candidate in SAMPLE_CONFIG_FILENAMES:
        if Path(candidate).is_file():
            return candidate
    return None


def load_yaml_config(path: str) -> dict[str, Any]:
    yaml = import_yaml()
    config_path = Path(path)

    if not config_path.is_file():
        raise RuntimeError(f"Config file not found: {path}")

    try:
        with config_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except OSError as exc:
        raise RuntimeError(f"Cannot read config file {path}: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Config file is not valid YAML: {path} ({exc})") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Config file must contain a top-level mapping/object.")

    return payload


def get_config_section(config: dict[str, Any], key: str) -> dict[str, Any]:
    value = config.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise RuntimeError(f"Config key '{key}' must be a mapping/object.")
    return value


def get_config_value(config: dict[str, Any], key: str, *aliases: str) -> Any:
    names = (key, *aliases)
    options = get_config_section(config, "options")

    for name in names:
        if name in config:
            return config[name]
    for name in names:
        if name in options:
            return options[name]

    return None


def parse_config_list(value: Any, key_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        items = value
    elif isinstance(value, dict):
        raise RuntimeError(f"Config key '{key_name}' must be a string or list of strings.")
    else:
        items = [value]

    result: list[str] = []
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            result.append(text)

    return result


def parse_config_bool(value: Any, key_name: str) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    raise RuntimeError(f"Config key '{key_name}' must be a boolean value.")


def parse_config_int(value: Any, key_name: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise RuntimeError(f"Config key '{key_name}' must be an integer.")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError as exc:
            raise RuntimeError(f"Config key '{key_name}' must be an integer.") from exc
    raise RuntimeError(f"Config key '{key_name}' must be an integer.")


def parse_config_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_config_targets(value: Any) -> list[TargetSpec]:
    """Parse targets from config. Supports both strings (simple paths) and objects (with reserve_mb)."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        items = value
    else:
        items = [value]

    specs: list[TargetSpec] = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, str):
            # Simple string target (e.g., "/storage/external_sd1")
            text = item.strip()
            if text:
                specs.append(TargetSpec(path=text, reserve_mb=None))
        elif isinstance(item, dict):
            # Object target with optional reserve_mb (e.g., {"path": "/storage/external_sd1", "reserve_mb": 512})
            path = item.get("path")
            if path:
                path = str(path).strip()
                if path:
                    reserve_mb = item.get("reserve_mb")
                    if reserve_mb is not None:
                        reserve_mb = parse_config_int(reserve_mb, "targets[].reserve_mb")
                    specs.append(TargetSpec(path=path, reserve_mb=reserve_mb))
        else:
            text = str(item).strip()
            if text:
                specs.append(TargetSpec(path=text, reserve_mb=None))

    return specs


def pick_value(cli_value: Any, config_value: Any, default: Any) -> Any:
    if cli_value is not None:
        return cli_value
    if config_value is not None:
        return config_value
    return default


def iter_local_files(spec: SourceSpec) -> Iterator[SourceFile]:
    root = Path(spec.root)

    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            file_path = Path(dirpath) / filename
            try:
                stat_info = file_path.stat()
            except OSError as exc:
                print(f"[WARN] Cannot stat file: {file_path} ({exc})", file=sys.stderr)
                continue

            relative_path = file_path.relative_to(root).as_posix()
            yield SourceFile(
                source_alias=spec.alias,
                source_root=spec.root,
                source_path=str(file_path),
                relative_path=relative_path,
                size=stat_info.st_size,
                mtime=stat_info.st_mtime,
                kind="local",
            )


def iter_smb_files(spec: SourceSpec, smbclient) -> Iterator[SourceFile]:
    root = spec.root.rstrip("\\/")

    try:
        walker = smbclient.walk(root)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Cannot access SMB source {spec.root}: {exc}") from exc

    for dirpath, _, filenames in walker:
        for filename in filenames:
            smb_path = ntpath.join(dirpath, filename)
            try:
                stat_info = smbclient.stat(smb_path)
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] Cannot stat SMB file: {smb_path} ({exc})", file=sys.stderr)
                continue

            relative_path = ntpath.relpath(smb_path, root).replace("\\", "/")
            yield SourceFile(
                source_alias=spec.alias,
                source_root=spec.root,
                source_path=smb_path,
                relative_path=relative_path,
                size=stat_info.st_size,
                mtime=getattr(stat_info, "st_mtime", None),
                kind="smb",
            )


def iter_source_files(specs: list[SourceSpec], smbclient) -> Iterator[SourceFile]:
    for spec in specs:
        if spec.kind == "local":
            yield from iter_local_files(spec)
        else:
            yield from iter_smb_files(spec, smbclient)


def decode_mount_field(field: str) -> str:
    return (
        field.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def is_probably_internal_storage(path: str) -> bool:
    normalized = os.path.realpath(path).lower()
    for prefix in INTERNAL_STORAGE_PREFIXES:
        if normalized == prefix or normalized.startswith(prefix + "/"):
            return True
    return False


def detect_android_external_mounts() -> list[str]:
    mounts: set[str] = set()
    proc_mounts = Path("/proc/mounts")
    if not proc_mounts.exists():
        return []

    try:
        lines = proc_mounts.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []

    for line in lines:
        parts = line.split()
        if len(parts) < 2:
            continue

        mount_point = decode_mount_field(parts[1])
        if not mount_point.startswith("/storage/"):
            continue
        if is_probably_internal_storage(mount_point):
            continue

        real_mount = os.path.realpath(mount_point)
        if not os.path.isdir(real_mount):
            continue
        if not os.access(real_mount, os.W_OK):
            continue

        mounts.add(real_mount)

    return sorted(mounts)


def resolve_targets(raw_targets: list[TargetSpec], auto_detect: bool, allow_internal: bool) -> list[TargetSpec]:
    candidates: list[TargetSpec] = list(raw_targets)
    if auto_detect or not candidates:
        auto_detected = detect_android_external_mounts()
        candidates.extend(TargetSpec(path=t, reserve_mb=None) for t in auto_detected)

    if not candidates:
        raise RuntimeError(
            "No target folders were provided and no external Android storage targets were detected."
        )

    resolved: list[TargetSpec] = []
    seen: set[str] = set()

    for candidate in candidates:
        expanded = os.path.expanduser(candidate.path)
        real_path = os.path.realpath(expanded)

        if not os.path.isdir(real_path):
            print(f"[WARN] Skipping missing target folder: {candidate.path}", file=sys.stderr)
            continue
        if not os.access(real_path, os.W_OK):
            print(f"[WARN] Skipping non-writable target folder: {real_path}", file=sys.stderr)
            continue
        if is_probably_internal_storage(real_path) and not allow_internal:
            print(f"[WARN] Skipping internal storage target: {real_path}", file=sys.stderr)
            continue
        if real_path in seen:
            continue

        seen.add(real_path)
        resolved.append(TargetSpec(path=real_path, reserve_mb=candidate.reserve_mb))

    if not resolved:
        raise RuntimeError("No usable target folders remain after validation.")

    return resolved


def build_target_states(targets: list[TargetSpec], reserve_mb: int) -> list[TargetState]:
    states: list[TargetState] = []

    for target_spec in targets:
        # Use per-target reserve if specified, otherwise use global reserve
        target_reserve_mb = target_spec.reserve_mb if target_spec.reserve_mb is not None else reserve_mb
        reserve_bytes = max(target_reserve_mb, 0) * 1024 * 1024
        
        usage = shutil.disk_usage(target_spec.path)
        usable = max(usage.free - reserve_bytes, 0)
        states.append(
            TargetState(
                root=Path(target_spec.path),
                free_bytes=usage.free,
                reserve_bytes=reserve_bytes,
                usable_bytes=usable,
                remaining_bytes=usable,
            )
        )

    return states


def split_relative_path(relative_path: str) -> list[str]:
    parts = [part for part in re.split(r"[\\/]+", relative_path) if part and part not in {".", ".."}]
    if not parts:
        return [sanitize_alias(relative_path)]
    return parts


def destination_relative_path_for_entry(entry: SourceFile) -> str:
    return entry.relative_path.replace("\\", "/")


def make_file_signature(relative_path: str, size: int) -> FileSignature:
    normalized = "/".join(split_relative_path(relative_path))
    return normalized, size


def source_signature(entry: SourceFile) -> FileSignature:
    return make_file_signature(destination_relative_path_for_entry(entry), entry.size)


def destination_path_for_entry(target_root: Path, entry: SourceFile) -> Path:
    destination = target_root
    for part in split_relative_path(destination_relative_path_for_entry(entry)):
        destination = destination / part
    return destination


def scan_destination_inventory(
    target_paths: list[str],
) -> tuple[
    list[DestinationFile],
    dict[Path, tuple[int, int]],
    dict[FileSignature, list[DestinationFile]],
]:
    inventory: list[DestinationFile] = []
    per_target_stats: dict[Path, tuple[int, int]] = {}
    grouped: dict[FileSignature, list[DestinationFile]] = {}

    for target in target_paths:
        root = Path(target)
        file_count = 0
        total_bytes = 0

        for dirpath, _, filenames in os.walk(root):
            for filename in filenames:
                file_path = Path(dirpath) / filename
                try:
                    stat_info = file_path.stat()
                except OSError as exc:
                    print(
                        f"[WARN] Cannot stat destination file: {file_path} ({exc})",
                        file=sys.stderr,
                    )
                    continue

                relative_path = file_path.relative_to(root).as_posix()
                entry = DestinationFile(
                    target_root=root,
                    path=file_path,
                    relative_path=relative_path,
                    size=stat_info.st_size,
                )
                inventory.append(entry)

                file_count += 1
                total_bytes += entry.size

                signature = make_file_signature(relative_path, entry.size)
                grouped.setdefault(signature, []).append(entry)

        per_target_stats[root] = (file_count, total_bytes)

    return inventory, per_target_stats, grouped


def find_duplicate_destination_files(
    grouped_inventory: dict[FileSignature, list[DestinationFile]],
    target_priority: list[str],
) -> tuple[list[DestinationFile], int, int]:
    duplicates_to_remove: list[DestinationFile] = []
    queued_paths: set[str] = set()
    duplicate_groups = 0
    reclaimable_bytes = 0

    # Build priority map: earlier targets in list get lower index (higher priority for keeping)
    priority_map = {target: idx for idx, target in enumerate(target_priority)}

    for entries in grouped_inventory.values():
        if len(entries) <= 1:
            continue

        duplicate_groups += 1
        # Sort by target priority: lower index (earlier in config) comes first and is kept
        def sort_key(item: DestinationFile) -> tuple[int, str]:
            target_path = str(item.target_root)
            priority_idx = priority_map.get(target_path, float('inf'))
            return (priority_idx, target_path)

        ordered = sorted(entries, key=sort_key)
        for duplicate in ordered[1:]:
            if duplicate.path.name.lower() in PROTECTED_DEDUPE_FILENAMES:
                continue
            duplicate_key = os.path.realpath(str(duplicate.path)).lower()
            if duplicate_key in queued_paths:
                continue
            queued_paths.add(duplicate_key)
            duplicates_to_remove.append(duplicate)
            reclaimable_bytes += duplicate.size

    return duplicates_to_remove, duplicate_groups, reclaimable_bytes


def apply_duplicate_reclaim_to_targets(
    target_states: list[TargetState],
    duplicates_to_remove: list[DestinationFile],
) -> None:
    by_root = {state.root: state for state in target_states}

    for duplicate in duplicates_to_remove:
        state = by_root.get(duplicate.target_root)
        if state is None:
            continue
        state.remaining_bytes += duplicate.size
        state.usable_bytes += duplicate.size


def remove_duplicate_destination_files(
    duplicates_to_remove: list[DestinationFile],
    allowed_target_roots: list[Path],
    verbose: bool,
) -> tuple[int, int, int]:
    removed_count = 0
    reclaimed_bytes = 0
    failed_count = 0
    processed_paths: set[str] = set()

    allowed_roots = [Path(os.path.realpath(str(root))) for root in allowed_target_roots]

    def is_under_allowed_roots(path: Path) -> bool:
        real_path = Path(os.path.realpath(str(path)))
        for root in allowed_roots:
            try:
                real_path.relative_to(root)
                return True
            except ValueError:
                continue
        return False

    for duplicate in duplicates_to_remove:
        duplicate_key = os.path.realpath(str(duplicate.path)).lower()
        if duplicate_key in processed_paths:
            if verbose:
                print(f"[DEDUPE] Skipped already processed duplicate: {duplicate.path}")
            continue
        processed_paths.add(duplicate_key)

        file_name = duplicate.path.name.lower()
        if file_name in PROTECTED_DEDUPE_FILENAMES:
            if verbose:
                print(f"[DEDUPE] Skipped protected file: {duplicate.path}")
            continue

        if not is_under_allowed_roots(duplicate.path):
            failed_count += 1
            print(
                f"[WARN] Refusing to remove file outside configured targets: {duplicate.path}",
                file=sys.stderr,
            )
            continue

        try:
            duplicate.path.unlink()
            removed_count += 1
            reclaimed_bytes += duplicate.size
            if verbose:
                print(f"[DEDUPE] Removed duplicate: {duplicate.path}")
        except FileNotFoundError:
            if verbose:
                print(f"[DEDUPE] Already absent, skipping: {duplicate.path}")
        except OSError as exc:
            failed_count += 1
            print(
                f"[WARN] Could not remove duplicate file: {duplicate.path} ({exc})",
                file=sys.stderr,
            )

    return removed_count, reclaimed_bytes, failed_count


def is_no_space_error(exc: Exception) -> bool:
    if isinstance(exc, OSError) and exc.errno == errno.ENOSPC:
        return True
    message = str(exc).lower()
    return "no space left" in message or "disk full" in message


def choose_target(
    target_states: list[TargetState],
    file_size: int,
    rng: random.Random,
) -> Optional[TargetState]:
    candidates = [state for state in target_states if state.remaining_bytes >= file_size]
    if not candidates:
        return None

    best_gap = min(state.remaining_bytes - file_size for state in candidates)
    best_candidates = [
        state for state in candidates if (state.remaining_bytes - file_size) == best_gap
    ]
    return rng.choice(best_candidates)


def build_plan(
    files: Iterable[SourceFile],
    target_states: list[TargetState],
    rng: random.Random,
    max_files: Optional[int],
    existing_signatures: set[FileSignature],
    existing_relative_paths: set[str],
) -> tuple[list[PlannedCopy], int, int]:
    pool = list(files)
    rng.shuffle(pool)

    plan: list[PlannedCopy] = []
    unplaced_count = 0
    skipped_existing_count = 0
    seen_signatures = set(existing_signatures)
    seen_relative_paths = set(existing_relative_paths)

    for entry in pool:
        if max_files is not None and len(plan) >= max_files:
            break

        relative_path = "/".join(split_relative_path(destination_relative_path_for_entry(entry)))
        if relative_path in seen_relative_paths:
            skipped_existing_count += 1
            continue

        signature = source_signature(entry)
        if signature in seen_signatures:
            skipped_existing_count += 1
            continue

        target = choose_target(target_states, entry.size, rng)
        if target is None:
            unplaced_count += 1
            continue

        destination = destination_path_for_entry(target.root, entry)

        target.remaining_bytes -= entry.size
        target.planned_bytes += entry.size
        target.planned_files += 1
        seen_signatures.add(signature)
        seen_relative_paths.add(relative_path)

        plan.append(PlannedCopy(entry=entry, target_root=target.root, destination_path=destination))

    return plan, unplaced_count, skipped_existing_count


def next_available_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    index = 1

    while True:
        candidate = path.with_name(f"{stem} ({index}){suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def write_plan_file(plan_path: str, plan: list[PlannedCopy]) -> None:
    output = Path(plan_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8") as handle:
        for item in plan:
            record = {
                "source": item.entry.source_path,
                "destination": str(item.destination_path),
                "size_bytes": item.entry.size,
                "source_alias": item.entry.source_alias,
            }
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def ordered_copy_targets(
    target_states: list[TargetState],
    preferred_root: Path,
    file_size: int,
) -> list[TargetState]:
    preferred: Optional[TargetState] = None
    others: list[TargetState] = []

    for state in target_states:
        if state.root == preferred_root:
            preferred = state
        else:
            others.append(state)

    viable_others = sorted(
        [state for state in others if state.remaining_bytes >= file_size],
        key=lambda state: state.remaining_bytes,
        reverse=True,
    )

    ordered: list[TargetState] = []
    if preferred is not None and preferred.remaining_bytes >= file_size:
        ordered.append(preferred)
    ordered.extend(viable_others)
    return ordered


def execute_plan(
    plan: list[PlannedCopy],
    target_states: list[TargetState],
    smbclient,
    overwrite: bool,
    verbose: bool,
) -> tuple[int, int, int, int]:
    copied_files = 0
    copied_bytes = 0
    skipped_existing = 0
    failed_files = 0

    for index, item in enumerate(plan, start=1):
        attempted_states = ordered_copy_targets(target_states, item.target_root, item.entry.size)
        if not attempted_states:
            failed_files += 1
            print(
                f"[ERROR] No target has enough remaining space for: {item.entry.source_path} "
                + f"({format_bytes(item.entry.size)})",
                file=sys.stderr,
            )
            continue

        copied_this_file = False
        skipped_this_file = False
        exhausted_space_failures = 0

        for state in attempted_states:
            destination = destination_path_for_entry(state.root, item.entry)

            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                print(
                    f"[ERROR] Cannot create destination folder: {destination.parent} ({exc})",
                    file=sys.stderr,
                )
                break

            candidate_destination = destination
            if candidate_destination.exists() and not overwrite:
                try:
                    if candidate_destination.stat().st_size == item.entry.size:
                        skipped_existing += 1
                        skipped_this_file = True
                        if verbose:
                            print(f"[SKIP] Already exists: {candidate_destination}")
                        break
                except OSError:
                    pass
                candidate_destination = next_available_path(candidate_destination)

            try:
                if item.entry.kind == "local":
                    shutil.copy2(item.entry.source_path, candidate_destination)
                else:
                    if smbclient is None:
                        raise RuntimeError("SMB client not initialized")
                    with smbclient.open_file(item.entry.source_path, mode="rb") as source_handle, open(
                        candidate_destination, "wb"
                    ) as destination_handle:
                        shutil.copyfileobj(source_handle, destination_handle, CHUNK_SIZE)
                    if item.entry.mtime is not None:
                        os.utime(candidate_destination, (time.time(), item.entry.mtime))

                copied_files += 1
                copied_bytes += item.entry.size
                state.remaining_bytes = max(state.remaining_bytes - item.entry.size, 0)
                copied_this_file = True

                if verbose or index % 25 == 0:
                    print(
                        f"[COPY] {index}/{len(plan)} files | Copied {format_bytes(copied_bytes)}"
                    )
                break

            except Exception as exc:  # noqa: BLE001
                if is_no_space_error(exc):
                    exhausted_space_failures += 1
                    state.remaining_bytes = 0
                    print(
                        f"[WARN] Target is full, trying next destination: {state.root} ({exc})",
                        file=sys.stderr,
                    )
                    continue

                print(
                    f"[ERROR] Copy failed: {item.entry.source_path} -> {candidate_destination} ({exc})",
                    file=sys.stderr,
                )
                break

        if skipped_this_file or copied_this_file:
            continue

        failed_files += 1
        if exhausted_space_failures > 0:
            print(
                f"[ERROR] Could not copy file, all attempted destinations are full: {item.entry.source_path}",
                file=sys.stderr,
            )

    return copied_files, copied_bytes, skipped_existing, failed_files


def print_target_summary(target_states: list[TargetState]) -> None:
    print("Target summary:")
    for state in target_states:
        print(
            "  "
            + f"{state.root} | free={format_bytes(state.free_bytes)} "
            + f"reserve={format_bytes(state.reserve_bytes)} "
            + f"planned={format_bytes(state.planned_bytes)} "
            + f"remaining={format_bytes(state.remaining_bytes)}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fill one or more target folders with a random selection of files from source folders "
            "until targets are full. Internal Android storage is excluded by default."
        )
    )
    parser.add_argument(
        "--config",
        default=None,
        help=(
            "Path to YAML config file. If omitted, config.yaml or config.yml is used when present. "
            "CLI flags override config values."
        ),
    )
    parser.add_argument(
        "--source",
        action="append",
        required=False,
        help="Source folder path. Repeat for multiple sources. Supports local paths and SMB/UNC paths.",
    )
    parser.add_argument(
        "--target",
        action="append",
        help="Target folder path. Repeat for multiple targets. If omitted, Android external mounts are auto-detected.",
    )
    parser.add_argument(
        "--auto-detect-targets",
        action="store_true",
        default=None,
        help="Add detected Android external mount points to the target list.",
    )
    parser.add_argument(
        "--list-targets",
        action="store_true",
        default=None,
        help="List detected Android external storage mount points and exit.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        default=None,
        help="Perform file copies. Default behavior is dry-run planning only.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for repeatable file selection.",
    )
    parser.add_argument(
        "--reserve-mb",
        type=int,
        default=None,
        help="Reserve this much free space on each target (default: 256 MB).",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional cap on number of files to include in the plan.",
    )
    parser.add_argument(
        "--plan-output",
        default=None,
        help="Optional output file path for the generated copy plan (JSON lines).",
    )
    parser.add_argument(
        "--allow-internal",
        action="store_true",
        default=None,
        help="Allow internal storage targets (/storage/emulated, /sdcard).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=None,
        help="Overwrite destination files with the same name. Default keeps existing files.",
    )
    parser.add_argument(
        "--smb-username",
        default=None,
        help="SMB username. Can also be set via SMB_USERNAME environment variable.",
    )
    parser.add_argument(
        "--smb-password",
        default=None,
        help="SMB password. Can also be set via SMB_PASSWORD environment variable.",
    )
    parser.add_argument(
        "--smb-domain",
        default=None,
        help="SMB domain/workgroup. Can also be set via SMB_DOMAIN environment variable.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=None,
        help="Print more progress details.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        config_path = args.config or discover_default_config_path()
        if not config_path and not args.source and not args.list_targets:
            sample_config_path = discover_sample_config_path()
            if sample_config_path:
                print(
                    "[WARN] No config.yaml/config.yml found. "
                    + f"Copy {sample_config_path} to config.yaml and edit values.",
                    file=sys.stderr,
                )

        config: dict[str, Any] = {}
        if config_path:
            config = load_yaml_config(config_path)
            print(f"Using config file: {config_path}")

        source_values = args.source
        if not source_values:
            source_values = parse_config_list(
                get_config_value(config, "sources", "source"),
                "sources",
            )

        target_specs: list[TargetSpec] = []
        if args.target:
            # CLI targets are strings, convert to TargetSpec
            target_specs = [TargetSpec(path=t, reserve_mb=None) for t in args.target]
        else:
            # Load from config (can be strings or objects)
            target_specs = parse_config_targets(
                get_config_value(config, "targets", "target"),
            )

        list_targets = pick_value(
            args.list_targets,
            parse_config_bool(get_config_value(config, "list_targets"), "list_targets"),
            False,
        )
        auto_detect_targets = pick_value(
            args.auto_detect_targets,
            parse_config_bool(
                get_config_value(config, "auto_detect_targets", "auto-detect-targets"),
                "auto_detect_targets",
            ),
            False,
        )
        execute = pick_value(
            args.execute,
            parse_config_bool(get_config_value(config, "execute"), "execute"),
            False,
        )
        allow_internal = pick_value(
            args.allow_internal,
            parse_config_bool(get_config_value(config, "allow_internal", "allow-internal"), "allow_internal"),
            False,
        )
        overwrite = pick_value(
            args.overwrite,
            parse_config_bool(get_config_value(config, "overwrite"), "overwrite"),
            False,
        )
        verbose = pick_value(
            args.verbose,
            parse_config_bool(get_config_value(config, "verbose"), "verbose"),
            False,
        )
        reserve_mb = pick_value(
            args.reserve_mb,
            parse_config_int(get_config_value(config, "reserve_mb", "reserve-mb"), "reserve_mb"),
            DEFAULT_RESERVE_MB,
        )
        seed = pick_value(
            args.seed,
            parse_config_int(get_config_value(config, "seed"), "seed"),
            None,
        )
        max_files = pick_value(
            args.max_files,
            parse_config_int(get_config_value(config, "max_files", "max-files"), "max_files"),
            None,
        )
        plan_output = pick_value(
            args.plan_output,
            parse_config_str(get_config_value(config, "plan_output", "plan-output")),
            None,
        )

        smb_config = get_config_section(config, "smb")
        smb_username = pick_value(
            args.smb_username,
            parse_config_str(smb_config.get("username") or smb_config.get("user")),
            None,
        )
        smb_password = pick_value(
            args.smb_password,
            parse_config_str(smb_config.get("password")),
            None,
        )
        smb_domain = pick_value(
            args.smb_domain,
            parse_config_str(smb_config.get("domain") or smb_config.get("workgroup")),
            None,
        )

        if list_targets:
            detected_targets = detect_android_external_mounts()
            if not detected_targets:
                print("No external Android storage mount points detected.")
            else:
                print("Detected external Android storage mount points:")
                for target in detected_targets:
                    print(f"  {target}")
            return 0

        if not source_values:
            parser.error("--source is required unless --list-targets is used.")

        source_specs = build_source_specs(source_values)

        requires_smb = any(spec.kind == "smb" for spec in source_specs)
        smbclient = None
        if requires_smb:
            smbclient = import_smbclient()
            configure_smb_client(smbclient, smb_username, smb_password, smb_domain)

        target_specs = resolve_targets(
            raw_targets=target_specs,
            auto_detect=auto_detect_targets,
            allow_internal=allow_internal,
        )
        target_states = build_target_states(target_specs, reserve_mb)
        target_paths = [spec.path for spec in target_specs]

        print("Scanning destination files...")
        destination_inventory, destination_stats, grouped_destination_files = scan_destination_inventory(
            target_paths
        )
        existing_signatures = set(grouped_destination_files.keys())
        existing_relative_paths = {
            "/".join(split_relative_path(item.relative_path)) for item in destination_inventory
        }
        duplicates_to_remove, duplicate_groups, reclaimable_bytes = find_duplicate_destination_files(
            grouped_destination_files, target_paths
        )

        destination_total_bytes = sum(item.size for item in destination_inventory)
        print(
            f"Destination files found: {len(destination_inventory)} "
            + f"({format_bytes(destination_total_bytes)})"
        )
        for target_path in target_paths:
            root = Path(target_path)
            file_count, target_bytes = destination_stats.get(root, (0, 0))
            print(
                f"  {root} | files={file_count} | existing={format_bytes(target_bytes)}"
            )

        if duplicates_to_remove:
            print(
                f"Duplicate files across destinations: {len(duplicates_to_remove)} "
                + f"in {duplicate_groups} groups ({format_bytes(reclaimable_bytes)} reclaimable)"
            )
            if execute:
                print("Removing duplicate destination files...")
                removed_count, reclaimed_bytes, failed_removals = remove_duplicate_destination_files(
                    duplicates_to_remove,
                    [Path(target_path) for target_path in target_paths],
                    verbose,
                )
                print(
                    f"Duplicate cleanup: removed={removed_count} "
                    + f"failed={failed_removals} reclaimed={format_bytes(reclaimed_bytes)}"
                )
                if removed_count > 0:
                    target_states = build_target_states(target_specs, reserve_mb)
            else:
                apply_duplicate_reclaim_to_targets(target_states, duplicates_to_remove)
                print(
                    "Dry-run mode: duplicate cleanup was simulated for capacity planning. "
                    + "Use --execute to apply removals."
                )
        else:
            print("No duplicate files found across destinations.")

        total_usable_bytes = sum(state.usable_bytes for state in target_states)
        if total_usable_bytes <= 0:
            raise RuntimeError(
                "Targets have no usable free space after reserve. Reduce --reserve-mb or free space first."
            )

        print("Scanning source files...")
        source_files = list(iter_source_files(source_specs, smbclient))
        if not source_files:
            raise RuntimeError("No files found in the source folders.")

        source_total_bytes = sum(item.size for item in source_files)
        rng = random.Random(seed)
        plan, unplaced_count, skipped_existing_count = build_plan(
            source_files,
            target_states,
            rng,
            max_files,
            existing_signatures,
            existing_relative_paths,
        )

        planned_total_bytes = sum(item.entry.size for item in plan)

        print(f"Source files found: {len(source_files)} ({format_bytes(source_total_bytes)})")
        print(f"Source files already present in destinations: {skipped_existing_count}")
        print(f"Files in random copy plan: {len(plan)} ({format_bytes(planned_total_bytes)})")
        print(f"Files that did not fit: {unplaced_count}")
        print_target_summary(target_states)

        if plan_output:
            write_plan_file(plan_output, plan)
            print(f"Plan written to: {plan_output}")

        if not execute:
            print("Dry-run complete. Re-run with --execute to copy files.")
            return 0

        print("Starting copy...")
        copied_files, copied_bytes, skipped_existing, failed_files = execute_plan(
            plan=plan,
            target_states=target_states,
            smbclient=smbclient,
            overwrite=overwrite,
            verbose=verbose,
        )

        print("Copy complete.")
        print(f"Copied files: {copied_files} ({format_bytes(copied_bytes)})")
        print(f"Skipped existing: {skipped_existing}")
        print(f"Failed copies: {failed_files}")

        return 0 if failed_files == 0 else 2

    except RuntimeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
