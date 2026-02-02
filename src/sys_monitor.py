from __future__ import annotations

import csv
import time
from datetime import datetime
from pathlib import Path

import psutil


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_header(csv_path: Path, header: list[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)


def get_uptime_seconds() -> float:
    # psutil boot_time is reliable cross-distro
    return time.time() - psutil.boot_time()


def top3_processes() -> tuple[list[str], list[str]]:
    """
    Returns:
      top_cpu: ["name(pid):cpu%", ...] length 3
      top_mem: ["name(pid):mem%", ...] length 3
    """
    procs = []
    for p in psutil.process_iter(["pid", "name"]):
        try:
            procs.append(p)
        except Exception:
            continue

    # Prime cpu%
    for p in procs:
        try:
            p.cpu_percent(None)
        except Exception:
            pass
    time.sleep(0.3)

    cpu_list = []
    mem_list = []
    for p in procs:
        try:
            name = p.info.get("name") or "unknown"
            pid = p.info.get("pid")
            cpu = p.cpu_percent(None)
            mem = p.memory_percent()
            cpu_list.append((name, pid, float(cpu)))
            mem_list.append((name, pid, float(mem)))
        except Exception:
            continue

    cpu_list.sort(key=lambda x: x[2], reverse=True)
    mem_list.sort(key=lambda x: x[2], reverse=True)

    top_cpu = [f"{n}({pid}):{v:.1f}%" for n, pid, v in cpu_list[:3]]
    top_mem = [f"{n}({pid}):{v:.2f}%" for n, pid, v in mem_list[:3]]

    while len(top_cpu) < 3:
        top_cpu.append("")
    while len(top_mem) < 3:
        top_mem.append("")

    return top_cpu, top_mem


def run_sys_monitor(csv_path: Path, interval_sec: int = 15) -> None:
    header = [
        "Timestamp",
        "CPU_Usage_Pct",
        "LoadAvg_1m",
        "LoadAvg_5m",
        "LoadAvg_15m",
        "Proc_Total",
        "Proc_Running",
        "Proc_Sleeping",
        "Mem_Total_MB",
        "Mem_Used_MB",
        "Mem_Avail_MB",
        "Mem_Usage_Pct",
        "Disk_Total_GB",
        "Disk_Used_GB",
        "Disk_Free_GB",
        "Disk_Usage_Pct",
        "Uptime_Sec",
        "TopCPU_1",
        "TopCPU_2",
        "TopCPU_3",
        "TopMem_1",
        "TopMem_2",
        "TopMem_3",
    ]
    ensure_header(csv_path, header)

    # psutil loadavg (Linux)
    while True:
        ts = now_str()

        cpu_pct = psutil.cpu_percent(interval=1)

        try:
            la1, la5, la15 = psutil.getloadavg()
        except Exception:
            la1 = la5 = la15 = 0.0

        total = 0
        running = 0
        sleeping = 0
        for p in psutil.process_iter(["status"]):
            try:
                total += 1
                st = p.info.get("status")
                if st == psutil.STATUS_RUNNING:
                    running += 1
                elif st in (psutil.STATUS_SLEEPING, psutil.STATUS_DISK_SLEEP):
                    sleeping += 1
            except Exception:
                continue

        vm = psutil.virtual_memory()
        mem_total = vm.total / (1024 * 1024)
        mem_used = vm.used / (1024 * 1024)
        mem_avail = vm.available / (1024 * 1024)
        mem_pct = vm.percent

        du = psutil.disk_usage("/")
        disk_total = du.total / (1024**3)
        disk_used = du.used / (1024**3)
        disk_free = du.free / (1024**3)
        disk_pct = du.percent

        uptime = get_uptime_seconds()

        top_cpu, top_mem = top3_processes()

        row = [
            ts,
            f"{cpu_pct:.2f}",
            f"{la1:.2f}",
            f"{la5:.2f}",
            f"{la15:.2f}",
            str(total),
            str(running),
            str(sleeping),
            f"{mem_total:.2f}",
            f"{mem_used:.2f}",
            f"{mem_avail:.2f}",
            f"{mem_pct:.2f}",
            f"{disk_total:.2f}",
            f"{disk_used:.2f}",
            f"{disk_free:.2f}",
            f"{disk_pct:.2f}",
            f"{uptime:.2f}",
            top_cpu[0],
            top_cpu[1],
            top_cpu[2],
            top_mem[0],
            top_mem[1],
            top_mem[2],
        ]

        with csv_path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)

        time.sleep(interval_sec)
