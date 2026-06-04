with open('app.py', 'rb') as f:
    raw = f.read()

# Replace bad bytes directly
raw = raw.replace(b'\xc3\xb0\xc5\xb8\xc5\x92\xc2\xb1', b'&#127793;')
raw = raw.replace(b'\xe2\x98\x81\xef\xb8\x8f', b'&#9729;')
raw = raw.replace(b'\xe2\x9a\xa1', b'&#9889;')
raw = raw.replace(b'\xf0\x9f\x94\x8b', b'&#128203;')
raw = raw.replace(b'\xf0\x9f\x94\x8d', b'&#128269;')
raw = raw.replace(b'\xf0\x9f\x94\x8a', b'&#128202;')
raw = raw.replace(b'\xf0\x9f\x92\xb0', b'&#128176;')
raw = raw.replace(b'\xe2\x9c\xa8', b'&#10024;')
raw = raw.replace(b'\xe2\x9c\x85', b'&#9989;')
raw = raw.replace(b'\xf0\x9f\xa7\xaa', b'&#129514;')
raw = raw.replace(b'\xf0\x9f\x93\x8b', b'&#128203;')
raw = raw.replace(b'\xe2\x80\x94', b'-')
raw = raw.replace(b'\xe2\x80\x93', b'-')
raw = raw.replace(b'\xe2\x80\x99', b"'")
raw = raw.replace(b'\xe2\x80\x9c', b'"')
raw = raw.replace(b'\xe2\x80\x9d', b'"')
raw = raw.replace(b'\xef\xb8\x8f', b'')

with open('app.py', 'wb') as f:
    f.write(raw)
print('Done fixing bytes!')

# Verify UTF-8
try:
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    print('OK - File is valid UTF-8!')
except Exception as e:
    print('Error:', e)
