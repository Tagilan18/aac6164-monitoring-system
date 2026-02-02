from __future__ import annotations
import time
import csv
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Dict

@dataclass
class FileMeta:
    path: str
    name: str
    ftype: str
    size: int
    perms: str
    owner: str
    group: str
    atime: float
    mtime: float
    ctime: float

def file_type(p: Path) -> str:
    if p.is_symlink():
        return "symlink"
    if p.is_dir():
        return "directory"
    return "regular"

def perms_str(mode: int) -> str:
    return oct(mode & 0o777)

def get_owner_group(st) -> tuple[str, str]:
    try:
        import pwd, grp
        return pwd.getpwuid(st.st_uid).pw_name, grp.getgrgid(st.st_gid).gr_name
    except Exception:
        return str(st.st_uid), str(st.st_gid)

def snapshot(directory: Path) -> Dict[str, FileMeta]:
    data: Dict[str, FileMeta] = {}
    for p in directory.rglob("*"):
        if not p.exists():
            continue
        try:
            st = p.lstat()
            owner, group = get_owner_group(st)
            data[str(p)] = FileMeta(
                path=str(p),
                name=p.name,
                ftype=file_type(p),
                size=st.st_size,
                perms=perms_str(st.st_mode),
                owner=owner,
                group=group,
                atime=st.st_atime,
                mtime=st.st_mtime,
                ctime=st.st_ctime,
            )
        except Exception:
            continue
    return data

def ensure_csv_header(csv_path: Path, fieldnames: list[str]) -> None:
    if not csv_path.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()

def log_event(csv_path: Path, row: dict) -> None:
    ensure_csv_header(csv_path, list(row.keys()))
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=list(row.keys())).writerow(row)

def compare_and_log(prev: Dict[str, FileMeta], curr: Dict[str, FileMeta], csv_path: Path) -> None:
    now = time.time()
    prev_keys = set(prev.keys())
    curr_keys = set(curr.keys())

    for k in sorted(curr_keys - prev_keys):
        m = curr[k]
        log_event(csv_path, {
            "event": "CREATED",
            "detected_ts": now,
            **asdict(m),
            "old_size": "",
            "new_size": m.size,
            "old_mtime": "",
            "new_mtime": m.mtime,
            "old_perms": "",
            "new_perms": m.perms,
        })

    for k in sorted(prev_keys - curr_keys):
        m = prev[k]
        log_event(csv_path, {
            "event": "DELETED",
            "detected_ts": now,
            "path": m.path,
            "name": m.name,
            "ftype": m.ftype,
            "size": m.size,
            "perms": m.perms,
            "owner": m.owner,
            "group": m.group,
            "atime": m.atime,
            "mtime": m.mtime,
            "ctime": m.ctime,
            "old_size": m.size,
            "new_size": "",
            "old_mtime": m.mtime,
            "new_mtime": "",
            "old_perms": m.perms,
            "new_perms": "",
        })

    for k in sorted(prev_keys & curr_keys):
        a = prev[k]
        b = curr[k]
        if (a.size != b.size) or (a.mtime != b.mtime) or (a.perms != b.perms):
            log_event(csv_path, {
                "event": "MODIFIED",
                "detected_ts": now,
                **asdict(b),
                "old_size": a.size,
                "new_size": b.size,
                "old_mtime": a.mtime,
                "new_mtime": b.mtime,
                "old_perms": a.perms,
                "new_perms": b.perms,
            })

def run_dir_monitor(directory: Path, csv_path: Path, interval_sec: int = 2) -> None:
    prev = snapshot(directory)
    while True:
        time.sleep(interval_sec)
        curr = snapshot(directory)
        compare_and_log(prev, curr, csv_path)
        prev = curr

