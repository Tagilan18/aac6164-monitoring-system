from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def generate_report(dir_csv: Path, sys_csv: Path, out_txt: Path) -> None:
    out_txt.parent.mkdir(parents=True, exist_ok=True)

    dir_rows = read_csv(dir_csv)
    sys_rows = read_csv(sys_csv)

    # Directory stats
    created = sum(1 for r in dir_rows if r.get("event") == "CREATED")
    deleted = sum(1 for r in dir_rows if r.get("event") == "DELETED")
    modified = sum(1 for r in dir_rows if r.get("event") == "MODIFIED")

    # System stats
    cpu_vals = [safe_float(r.get("CPU_Usage_Pct")) for r in sys_rows if r.get("CPU_Usage_Pct")]
    mem_vals = [safe_float(r.get("Mem_Usage_Pct")) for r in sys_rows if r.get("Mem_Usage_Pct")]
    disk_vals = [safe_float(r.get("Disk_Usage_Pct")) for r in sys_rows if r.get("Disk_Usage_Pct")]

    def avg(xs):
        return (sum(xs) / len(xs)) if xs else 0.0

    lines = []
    lines.append("AAC6164 Monitoring System - Final Report")
    lines.append("=======================================")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    lines.append("1) Directory Monitoring Summary")
    lines.append("------------------------------")
    lines.append(f"Total directory events: {len(dir_rows)}")
    lines.append(f"CREATED:  {created}")
    lines.append(f"DELETED:  {deleted}")
    lines.append(f"MODIFIED: {modified}")
    lines.append("")

    if dir_rows:
        lines.append("Latest 5 directory events:")
        for r in dir_rows[-5:]:
            lines.append(
                f"- {r.get('event','?')} | {r.get('name','?')} | type {r.get('ftype','?')} | size {r.get('size','?')} | perms {r.get('perms','?')} | owner {r.get('owner','?')}"
            )
        lines.append("")

    lines.append("2) System Performance Summary")
    lines.append("-----------------------------")
    lines.append(f"Total system samples: {len(sys_rows)}")
    lines.append(f"Average CPU usage:  {avg(cpu_vals):.2f}%")
    lines.append(f"Average MEM usage:  {avg(mem_vals):.2f}%")
    lines.append(f"Average DISK usage: {avg(disk_vals):.2f}%")
    lines.append("")

    if sys_rows:
        last = sys_rows[-1]
        lines.append("Latest system sample:")
        lines.append(f"- Timestamp: {last.get('Timestamp')}")
        lines.append(f"- CPU: {last.get('CPU_Usage_Pct')}% | LoadAvg: {last.get('LoadAvg_1m')},{last.get('LoadAvg_5m')},{last.get('LoadAvg_15m')}")
        lines.append(f"- MEM: {last.get('Mem_Usage_Pct')}% | DISK: {last.get('Disk_Usage_Pct')}%")
        lines.append(f"- Processes: total {last.get('Proc_Total')} | running {last.get('Proc_Running')} | sleeping {last.get('Proc_Sleeping')}")
        lines.append(f"- Top CPU: {last.get('TopCPU_1')}, {last.get('TopCPU_2')}, {last.get('TopCPU_3')}")
        lines.append(f"- Top MEM: {last.get('TopMem_1')}, {last.get('TopMem_2')}, {last.get('TopMem_3')}")
        lines.append("")

    lines.append("3) Files Generated")
    lines.append("------------------")
    lines.append(f"- Directory log CSV: {dir_csv}")
    lines.append(f"- System log CSV:    {sys_csv}")
    lines.append(f"- Final report TXT:  {out_txt}")
    lines.append("")

    out_txt.write_text("\n".join(lines), encoding="utf-8")
