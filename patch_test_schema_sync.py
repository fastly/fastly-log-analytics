with open("tests/models/test_schema_sync.py") as f:
    content = f.read()

replacement = """        # Call the repository with an empty source/table to get the default empty structure
        try:
            repo_result = repo_fn(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={}, **extra_args)
        except TypeError as e:
            if "got an unexpected keyword argument 'con'" in str(e):
                repo_result = repo_fn(con_factory=lambda: in_memory_duckdb, src=src, start_time=None, end_time=None, filters={}, **extra_args)
            else:
                raise"""

content = content.replace(
    "        repo_result = repo_fn(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={}, **extra_args)",
    replacement,
)

with open("tests/models/test_schema_sync.py", "w") as f:
    f.write(content)
