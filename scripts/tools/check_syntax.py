with open('deploy-cluster.ps1', 'r', encoding='utf-8') as f:
    content = f.read()

lines = content.split('\n')

for i, line in enumerate(lines, 1):
    in_single = False
    in_double = False
    escape = False
    for j, ch in enumerate(line):
        if escape:
            escape = False
            continue
        if ch == '`':
            escape = True
            continue
        if not in_single and not in_double:
            if ch == '"':
                in_double = True
            elif ch == "'":
                in_single = True
        else:
            if in_double and ch == '"':
                in_double = False
            elif in_single and ch == "'":
                in_single = False
    if in_single or in_double:
        print(f'Line {i}: possible unclosed quote')
        print(f'  {line[:100]}')

for i, line in enumerate(lines, 1):
    balance = 0
    in_str = False
    str_char = None
    for ch in line:
        if not in_str and ch in '"\'':
            in_str = True
            str_char = ch
        elif in_str and ch == str_char:
            in_str = False
        elif not in_str:
            if ch == '(':
                balance += 1
            elif ch == ')':
                balance -= 1
    if balance != 0:
        print(f'Line {i}: paren balance {balance}')
        print(f'  {line[:100]}')

for i, line in enumerate(lines, 1):
    balance = 0
    in_str = False
    str_char = None
    for ch in line:
        if not in_str and ch in '"\'':
            in_str = True
            str_char = ch
        elif in_str and ch == str_char:
            in_str = False
        elif not in_str:
            if ch == '{':
                balance += 1
            elif ch == '}':
                balance -= 1
    if balance != 0:
        print(f'Line {i}: brace balance {balance}')
        print(f'  {line[:100]}')
