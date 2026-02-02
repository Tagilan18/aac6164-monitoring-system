from __future__ import annotations

import threading
import time
from pathlib import Path

from dir_monitor import run_dir_monitor
from sys_monitor import run_sys_monitor
from report import generate_report

# Project root directory
BASE = Path(__file__).resolve().parent.parent

MONITORED_DIR = BASE / "monitored"
LOG_DIR = BASE / "output" / "logs"
REPORT_DIR = BASE / "output" / "reports"

DIR_CSV = LOG_DIR / "directory_events.csv"
SYS_CSV = LOG_DIR / "system_metrics.csv"
REPORT_TXT = REPORT_DIR / "final_report.txt"


def main() -> None:
    # Ensure directories exist
    MONITORED_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Start monitors
    t_dir = threading.Thread(
        target=run_dir_monitor,
        args=(MONITORED_DIR, DIR_CSV, 2),
        daemon=True
    )
    t_sys = threading.Thread(
        target=run_sys_monitor,
        args=(SYS_CSV, 15),
        daemon=True
    )

    print("=== AAC6164 Monitoring System ===")
    print(f"Monitored directory : {MONITORED_DIR}")
    print(f"Directory log       : {DIR_CSV}")
    print(f"System log          : {SYS_CSV}")
    print("Create / modify / delete files in monitored/")
    print("Press Ctrl + C to stop and generate report\n")

    t_dir.start()
    t_sys.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping monitors...")
        generate_report(DIR_CSV, SYS_CSV, REPORT_TXT)
        print(f"Report generated at: {REPORT_TXT}")


if __name__ == "__main__":
    main()
