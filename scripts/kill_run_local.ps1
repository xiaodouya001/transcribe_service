# Kill any run_local or transcribe_service.main processes
$procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" | 
    Where-Object { $_.CommandLine -match 'run_local|transcribe_service\.main' }
if ($procs) {
    foreach ($p in $procs) {
        Write-Host "Killing PID $($p.ProcessId): $($p.CommandLine.Substring(0, [Math]::Min(80, $p.CommandLine.Length)))..."
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Write-Host "Done."
} else {
    Write-Host "No run_local or transcribe_service.main processes found."
}
