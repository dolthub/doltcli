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
    assert len(expected) == len(actual), f"Unequal row counts: {len(expected)} != {len(actual)}"
    errors = []
    for k in expected[0].keys():
        if k.startswith("date"):
            exp = set([e[k][:10] for e in expected])
            act = set([a[k][:10] for a in actual])
        else:
            exp = set([e[k] for e in expected])
            act = set([a[k] for a in actual])
        if exp ^ act != set():
            errors.append(f"Unequal value sets: {exp}, {act}")

    error_str = "\n".join(errors)
    assert not errors, f"Failed with the following unequal columns:\n{error_str}"
