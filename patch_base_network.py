with open("backend/repositories/_base.py") as f:
    content = f.read()

# We need to change _eligible_rollup_window calls in try_network_speed_from_rollup, try_network_rtt_from_rollup, try_network_heatmap_from_rollup, try_network_geo_from_rollup

funcs = [
    "try_network_speed_from_rollup",
    "try_network_rtt_from_rollup",
    "try_network_heatmap_from_rollup",
    "try_network_geo_from_rollup",
]

for func in funcs:
    # Find the line 'win = self._eligible_rollup_window(' inside the function
    func_idx = content.find(f"def {func}")
    if func_idx == -1:
        continue

    win_idx = content.find("win = self._eligible_rollup_window(", func_idx)
    end_win_idx = content.find(")", win_idx) + 1

    # replace the call
    original = content[win_idx:end_win_idx]

    # add min_hours=0
    if "min_hours=0" not in original:
        replaced = original[:-1] + ", min_hours=0)"
        content = content[:win_idx] + replaced + content[end_win_idx:]

with open("backend/repositories/_base.py", "w") as f:
    f.write(content)
