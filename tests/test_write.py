import os

import pytest

from doltcli import CREATE, DoltException, read_rows, write_columns, write_file, write_rows
from tests.helpers import compare_rows_helper, write_dict_to_csv

# Note that we use string values here as serializing via CSV does preserve type information in any meaningful way
TEST_ROWS = [
    {"name": "Anna", "adjective": "tragic", "id": "1", "date_of_death": "1877-01-01"},
    {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
    {"name": "Oblonksy", "adjective": "buffoon", "id": "3", "date_of_death": ""},
]

TEST_COLUMNS = {
    "name": ["Anna", "Vronksy", "Oblonksy"],
    "adjective": ["tragic", "honorable", "buffoon"],
    "id": ["1", "2", "3"],
    "date_of_birth": ["1840-01-01", "1840-01-01", "1840-01-01"],
    "date_of_death": ["1877-01-01", "", ""],
}


def test_write_rows(init_empty_test_repo):
    dolt = init_empty_test_repo
    write_rows(dolt, "characters", TEST_ROWS, CREATE, ["id"])
    actual = read_rows(dolt, "characters")
    compare_rows_helper(TEST_ROWS, actual)


def test_update_rows(init_empty_test_repo):
    dolt = init_empty_test_repo
    write_rows(dolt, "characters", TEST_ROWS, CREATE, ["id"])

    new_row = {"name": "dick butkus", "adjective": "buffoon", "id": "3", "date_of_death": ""}

    write_rows(dolt, "characters", [new_row], "update", ["id"])
    actual = read_rows(dolt, "characters")
    exp = [
        {"name": "Anna", "adjective": "tragic", "id": "1", "date_of_death": "1877-01-01"},
        {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
        {"name": "dick butkus", "adjective": "buffoon", "id": "3", "date_of_death": ""},
    ]
    compare_rows_helper(exp, actual)


def test_replace_rows(init_empty_test_repo):
    dolt = init_empty_test_repo
    write_rows(dolt, "characters", TEST_ROWS, CREATE, ["id"])

    new_row = {"name": "dick butkus", "adjective": "buffoon", "id": "3", "date_of_death": ""}

    write_rows(dolt, "characters", [new_row], "replace", ["id"])
    actual = read_rows(dolt, "characters")
    exp = [
        {"name": "dick butkus", "adjective": "buffoon", "id": "3", "date_of_death": ""},
    ]
    compare_rows_helper(exp, actual)


def test_write_columns(init_empty_test_repo):
    dolt = init_empty_test_repo
    write_columns(dolt, "characters", TEST_COLUMNS, CREATE, ["id"])
    actual = read_rows(dolt, "characters")
    expected = [{} for _ in range(len(list(TEST_COLUMNS.values())[0]))]
    for col_name in TEST_COLUMNS.keys():
        for j, val in enumerate(TEST_COLUMNS[col_name]):
            expected[j][col_name] = val

    compare_rows_helper(expected, actual)


DICT_OF_LISTS_UNEVEN_LENGTHS = {"name": ["Roger", "Rafael", "Novak"], "rank": [1, 2]}


def test_write_columns_uneven(init_empty_test_repo):
    repo = init_empty_test_repo
    with pytest.raises(ValueError):
        write_columns(repo, "players", DICT_OF_LISTS_UNEVEN_LENGTHS, CREATE, ["name"])


def test_write_file_handle(init_empty_test_repo, tmp_path):
    tempfile = tmp_path / "test.csv"
    TEST_ROWS = [
        {"name": "Anna", "adjective": "tragic", "id": "1", "date_of_death": "1877-01-01"},
        {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
        {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
    ]
    write_dict_to_csv(TEST_ROWS, tempfile)
    dolt = init_empty_test_repo
    with pytest.raises(DoltException):
        write_file(
            dolt=dolt,
            table="characters",
            file_handle=open(tempfile),
            import_mode=CREATE,
            primary_key=["id"],
        )
    write_file(
        dolt=dolt,
        table="characters",
        file_handle=open(tempfile),
        import_mode=CREATE,
        primary_key=["id"],
        do_continue=True,
    )
    actual = read_rows(dolt, "characters")
    compare_rows_helper(TEST_ROWS[:2], actual)


def test_write_file(init_empty_test_repo, tmp_path):
    tempfile = tmp_path / "test.csv"
    TEST_ROWS = [
        {"name": "Anna", "adjective": "tragic", "id": "1", "date_of_death": "1877-01-01"},
        {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
        {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
    ]
    write_dict_to_csv(TEST_ROWS, tempfile)
    dolt = init_empty_test_repo
    write_file(
        dolt=dolt,
        table="characters",
        file=tempfile,
        import_mode=CREATE,
        primary_key=["id"],
        do_continue=True,
    )
    assert os.path.exists(tmp_path)
    actual = read_rows(dolt, "characters")
    compare_rows_helper(TEST_ROWS[:2], actual)


def test_write_file_errors(init_empty_test_repo, tmp_path):
    tempfile = tmp_path / "test.csv"
    TEST_ROWS = [
        {"name": "Anna", "adjective": "tragic", "id": "1", "date_of_death": "1877-01-01"},
        {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
        {"name": "Vronksy", "adjective": "honorable", "id": "2", "date_of_death": ""},
    ]
    write_dict_to_csv(TEST_ROWS, tempfile)
    dolt = init_empty_test_repo
    with pytest.raises(DoltException):
        write_file(
            dolt=dolt,
            table="characters",
            file_handle=open(tempfile),
            import_mode=CREATE,
            primary_key=["id"],
        )
    with pytest.raises(ValueError):
        write_file(
            dolt=dolt,
            table="characters",
            file_handle=open(tempfile),
            file=tempfile,
            import_mode=CREATE,
            primary_key=["id"],
        )
    with pytest.raises(ValueError):
        write_file(
            dolt=dolt,
            table="characters",
            import_mode=CREATE,
            primary_key=["id"],
        )
    with pytest.raises(ValueError):
        write_file(
            dolt=dolt,
            file_handle=tempfile,
            table="characters",
            import_mode=CREATE,
            primary_key=["id"],
        )
