with open("tests/test_response_contract.py") as f:
    content = f.read()

content = content.replace(
    "result = get_health(seeded_con, _src, None, None, {})",
    "result = get_health(lambda: seeded_con, _src, None, None, {})",
)

with open("tests/test_response_contract.py", "w") as f:
    f.write(content)
