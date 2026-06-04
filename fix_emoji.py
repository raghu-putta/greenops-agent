# -*- coding: utf-8 -*-
with open('app.py', 'rb') as f:
    raw = f.read()

content = raw.decode('utf-8', errors='replace')

# Fix all broken emoji - replace with HTML entities (safe, no encoding issues)
fixes = [
    ('\U0001f9ea', '&#129514;'),
    ('\u2601\ufe0f', '&#9729;'),
    ('\u26a1', '&#9889;'),
    ('\U0001f4cb', '&#128203;'),
    ('\U0001f50d', '&#128269;'),
    ('\U0001f4ca', '&#128202;'),
    ('\U0001f4b0', '&#128176;'),
    ('\u2728', '&#10024;'),
    ('\U0001f331', '&#127793;'),
    ('\U0001f916', '&#129302;'),
    ('\U0001f4be', '&#128190;'),
    ('\U0001f4dd', '&#128221;'),
    ('\u2705', '&#9989;'),
    ('\u23f3', '&#9203;'),
    ('\U0001f6a8', '&#128680;'),
]

for emoji, entity in fixes:
    if emoji in content:
        content = content.replace(emoji, entity)
        print('Fixed emoji to entity')

# Fix robot buttons - add proper emoji text
content = content.replace(
    '>Robot<br/>Alex<',
    '>&#129302;<br/>Alex<'
)
content = content.replace(
    '>Lady<br/>Aria<',
    '>&#128105;&#8205;&#128187;<br/>Aria<'
)
content = content.replace(
    '>Guy<br/>Max<',
    '>&#128104;&#8205;&#128187;<br/>Max<'
)
content = content.replace(
    '>Brain<br/>Nova<',
    '>&#129504;<br/>Nova<'
)
content = content.replace(
    '>Leaf<br/>Eco<',
    '>&#127807;<br/>Eco<'
)

# Fix robot avatar text
content = content.replace(
    '"robot-avatar" style="font-size:2rem;animation:robotBob 2s ease-in-out infinite;flex-shrink:0;">Robot<',
    '"robot-avatar" style="font-size:2rem;animation:robotBob 2s ease-in-out infinite;flex-shrink:0;">&#129302;<'
)

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(content)
print('OK - All fixed and saved!')
