import re

with open('commands.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Extract the categories JS array
m = re.search(r'const categories = \[(.*?)\];\s*\n\s*// Total command count', content, re.DOTALL)
if m:
    arr = m.group(1)
    cats = re.findall(r'name:\s*"(.*?)"', arr)
    categories = []
    commands = []
    in_commands = False
    for name in cats:
        if name in ['String','List','Hash','Set','Sorted Set','Bitmap','HyperLogLog','Geo','Stream','Key Management','Transaction','Scripting','Pub/Sub','ACL','Client','Server/Admin','Replication','Sentinel','Cluster','Module']:
            in_commands = True
            categories.append(name)
        elif in_commands:
            commands.append(name)
    print('Categories:', len(categories))
    print('Commands:', len(commands))
    for c in categories:
        cnt = len(re.findall(r'\{\s*name:\s*"' + re.escape(c) + r'"', arr))
        # Not useful; let's just count per category differently
        pass
else:
    print('Pattern not found')

# Better approach: count command objects
command_blocks = re.findall(r'\{\s*name:\s*"[^"]+"\s*,\s*syntax:', arr)
print('Command objects (regex):', len(command_blocks))
