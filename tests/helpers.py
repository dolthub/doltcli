import csv
from typing import List


def write_dict_to_csv(data, file):
    csv_columns = list(data[0].keys())
    with open(file, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def read_csv_to_dict(file):
    with open(file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)


def compare_rows_helper(expected: List[dict], actual: List[dict]):
    assert len(expected) == len(
        actual
    ), f"Unequal row counts: {len(expected)} != {len(actual)}"
    errors = []
    for l, r in zip(expected, actual):
        l_cols, r_cols = set(l.keys()), set(r.keys())
        assert l_cols == r_cols, f"Unequal sets of columns: {l_cols} != {r_cols}"
        for col in l_cols:
            l_val, r_val = l[col], r[col]
            if col.startswith("date"):
                l_val, r_val = l_val[:10], r_val[:10]
            if l_val != r_val and not (l_val is None and r_val == ""):
                errors.append(f"{col}: {l_val} != {r_val}")

    error_str = "\n".join(errors)
    assert not errors, f"Failed with the following unequal columns:\n{error_str}"
