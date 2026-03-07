"""Find Python processes that might be run_local (asr_ingest.demo.run_local)."""
import subprocess
import sys

try:
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
         "ForEach-Object { $_.ProcessId.ToString() + '|' + ($_.CommandLine or '') }"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    out = result.stdout or ""
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

pids_to_kill = []
for line in out.strip().splitlines():
    if "|" in line:
        pid, cmd = line.split("|", 1)
        # run_local or asr_ingest.main
        if "run_local" in cmd or ("asr_ingest" in cmd and "main" in cmd):
            pids_to_kill.append(pid)
            print(f"PID {pid}: {cmd[:100]}...")

if pids_to_kill:
    print(f"\n>>> Found {len(pids_to_kill)} run_local/main process(es). Kill with:")
    for pid in pids_to_kill:
        print(f"    taskkill /F /PID {pid}")
else:
    print("No run_local or asr_ingest.main processes found.")
