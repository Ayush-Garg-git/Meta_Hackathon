import subprocess
import sys

def verify_output():
    process = subprocess.Popen(
        [sys.executable, "inference.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    stdout_lines = []
    stderr_lines = []
    
    while True:
        stdout_line = process.stdout.readline()
        stderr_line = process.stderr.readline()
        
        if stdout_line:
            stdout_lines.append(stdout_line.strip())
            print(f"STDOUT: {stdout_line.strip()}")
        if stderr_line:
            stderr_lines.append(stderr_line.strip())
            print(f"STDERR: {stderr_line.strip()}")
            
        if not stdout_line and not stderr_line and process.poll() is not None:
            break
            
    print("\n--- Captured STDOUT ---")
    for line in stdout_lines:
        print(line)
        
    # Check for [START], [STEP], [END]
    has_start = any(line.startswith("[START]") for line in stdout_lines)
    has_step = any(line.startswith("[STEP]") for line in stdout_lines)
    has_end = any(line.startswith("[END]") for line in stdout_lines)
    
    print("\n--- Summary ---")
    print(f"Has [START]: {has_start}")
    print(f"Has [STEP]: {has_step}")
    print(f"Has [END]: {has_end}")
    
    if has_start and has_step and has_end:
        print("SUCCESS: All structured blocks found.")
    else:
        print("FAILURE: Missing some structured blocks.")
        sys.exit(1)

if __name__ == "__main__":
    verify_output()
