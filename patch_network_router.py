with open("backend/routers/network.py") as f:
    content = f.read()

content = content.replace(
    "from backend.repositories import get_runner\n    runner = get_runner(ctx)",
    "from backend.repositories._base import QueryRunner\n    runner = QueryRunner(ctx.con, ctx.source)",
)

with open("backend/routers/network.py", "w") as f:
    f.write(content)
