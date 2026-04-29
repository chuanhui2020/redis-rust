import subprocess
import sys
import os

with open('deploy-cluster.ps1', 'r', encoding='utf-8') as f:
    lines = f.readlines()

def test_lines(start, end):
    text = ''.join(lines[start:end+1]) + '\n'
    with open('__temp.ps1', 'w', encoding='utf-8') as f:
        f.write(text)
    try:
        result = subprocess.run(
            ['powershell', '-NoProfile', '-File', '__temp.ps1'],
            capture_output=True, text=True, timeout=5
        )
        has_error = 'ParserError' in result.stderr or 'UnexpectedCharactersAfterHereString' in result.stderr
        return not has_error, result.stderr[:200]
    except subprocess.TimeoutExpired:
        return True, "TIMEOUT"
    except Exception as e:
        return False, str(e)

total = len(lines)
print(f"Total lines: {total}")

# Test full file
ok, err = test_lines(0, total - 1)
print(f"Full file: ok={ok}, err={err[:100]}")
if ok:
    print("Full file syntax OK")
    sys.exit(0)

# Binary search
low, high = 0, total - 1
while low < high:
    mid = (low + high) // 2
    ok, err = test_lines(0, mid)
    print(f"  0..{mid}: ok={ok}")
    if ok:
        low = mid + 1
    else:
        high = mid

print(f"\nFirst error around line {low + 1}:")
start = max(0, low - 2)
end = min(total - 1, low + 2)
for i in range(start, end + 1):
    marker = ">>> " if i == low else "    "
    print(f"{marker}{i+1:4}: {lines[i].rstrip()}")
