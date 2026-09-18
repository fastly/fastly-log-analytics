def _patch():
    with open("backend/repositories/security.py") as f:
        content = f.read()

    content = content.replace(
        "tb = get_top_bots(con, src, start_time, end_time, filters, shared_temp_table=temp_table)",
        "tb = get_top_bots(con, src, start_time, end_time, filters or {}, shared_temp_table=temp_table)",
    )

    with open("backend/repositories/security.py", "w") as f:
        f.write(content)


_patch()
