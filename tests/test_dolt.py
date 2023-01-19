import csv
import os
import shutil
import tempfile
import uuid
from typing import List, Tuple

import pytest

from doltcli import (
    CREATE,
    UPDATE,
    Dolt,
    _execute,
    detach_head,
    read_rows,
    set_dolt_path,
    write_rows,
)
from tests.helpers import compare_rows_helper, read_csv_to_dict

BASE_TEST_ROWS = [{"name": "Rafael", "id": "1"}, {"name": "Novak", "id": "2"}]


def get_repo_path_tmp_path(path: str, subpath: str = None) -> Tuple[str, str]:
    if subpath:
        return os.path.join(path, subpath), os.path.join(path, subpath, ".dolt")
    else:
        return path, os.path.join(path, ".dolt")


@pytest.fixture
def create_test_data(tmp_path) -> str:
    path = os.path.join(tmp_path, str(uuid.uuid4()))
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(list(BASE_TEST_ROWS[0].keys()))
        for row in BASE_TEST_ROWS:
            writer.writerow(list(row.values()))
    yield path
    os.remove(path)


@pytest.fixture
def create_test_table(init_empty_test_repo: Dolt, create_test_data: str) -> Tuple[Dolt, str]:
    repo, _ = init_empty_test_repo, create_test_data
    repo.sql(
        query="""
        CREATE TABLE `test_players` (
            `name` LONGTEXT NOT NULL COMMENT 'tag:0',
            `id` BIGINT NOT NULL COMMENT 'tag:1',
            PRIMARY KEY (`id`)
        );
    """
    )
    data = BASE_TEST_ROWS
    write_rows(repo, "test_players", data, UPDATE, commit=False)
    yield repo, "test_players"

    if "test_players" in [table.name for table in repo.ls()]:
        _execute(["table", "rm", "test_players"], repo.repo_dir)


def test_init(tmp_path):
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert not os.path.exists(repo_data_dir)
    Dolt.init(repo_path)
    assert os.path.exists(repo_data_dir)
    shutil.rmtree(repo_data_dir)


def test_bad_repo_path(tmp_path):
    bad_repo_path = tmp_path
    with pytest.raises(ValueError):
        Dolt(bad_repo_path)


def test_commit(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    repo.add(test_table)
    before_commit_count = len(repo.log())
    repo.commit("Julianna, the very serious intellectual")
    assert repo.status().is_clean and len(repo.log()) == before_commit_count + 1


def test_head(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    assert list(repo.log().values())[0].ref == repo.head


@pytest.mark.xfail(reason="Dolt cli bug with --result-format")
def test_working(doltdb):
    db = Dolt(doltdb)
    assert db.head != db.working


def test_active_branch(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    assert "main" == repo.active_branch


def test_merge_fast_forward(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    message_one = "Base branch"
    message_two = "Other branch"
    message_merge = "merge"

    # commit the current working set to main
    repo.add(test_table)
    repo.commit(message_one)

    # create another branch from the working set
    repo.branch("other")

    # create a non-trivial commit against `other`
    repo.checkout("other")
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Juan Martin", 5)')
    repo.add(test_table)
    repo.commit(message_two)

    # merge
    repo.checkout("main")
    repo.merge("other", message_merge)

    commits = list(repo.log().values())
    fast_forward_commit = commits[0]
    parent = commits[1]

    assert isinstance(fast_forward_commit.parents, str)
    assert fast_forward_commit.message == message_two
    assert parent.message == message_one


def test_merge_conflict(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    message_one = "Base branch"
    message_two = "Base branch new data"
    message_three = "Other branch"
    message_merge = "merge"
    # commit the current working set to main
    repo.add(test_table)
    repo.commit(message_one)

    # create another branch from the working set
    repo.branch("other")

    # create a non-trivial commit against `main`
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Stan", 4)')
    repo.add(test_table)
    repo.commit(message_two)

    # create a non-trivial commit against `other`
    repo.checkout("other")
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Marin", 4)')
    repo.add(test_table)
    repo.commit(message_three)

    # merge
    repo.checkout("main")
    repo.merge("other", message_merge)

    commits = list(repo.log().values())
    head_of_main = commits[0]

    assert head_of_main.message == message_two


def test_dolt_log(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    message_one = "Julianna, the very serious intellectual"
    message_two = "Added Stan the Man"
    repo.add(test_table)
    repo.commit("Julianna, the very serious intellectual")
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Stan", 4)')
    repo.add(test_table)
    repo.commit(message_two)
    commits = list(repo.log().values())
    current_commit = commits[0]
    previous_commit = commits[1]
    assert current_commit.message == message_two
    assert previous_commit.message == message_one


def test_dolt_log_scope(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    message_one = "Julianna, the very serious intellectual"
    message_two = "Added Stan the Man"
    repo.add(test_table)
    repo.commit("Julianna, the very serious intellectual")
    repo.checkout("tmp_br", checkout_branch=True)
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Stan", 4)')
    repo.add(test_table)
    repo.commit(message_two)
    repo.checkout("main")
    commits = list(repo.log().values())
    current_commit = commits[0]
    _ = commits[1]
    assert current_commit.message == message_one


def test_dolt_log_number(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    _ = "Julianna, the very serious intellectual"
    message_two = "Added Stan the Man"
    repo.add(test_table)
    repo.commit("Julianna, the very serious intellectual")
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Stan", 4)')
    repo.add(test_table)
    repo.commit(message_two)

    commits = list(repo.log(number=1).values())

    assert len(commits) == 1
    current_commit = commits[0]
    assert current_commit.message == message_two


def test_dolt_single_commit_log(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    assert len(repo.log()) == 1


def test_dolt_log_commit(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    _ = "Julianna, the very serious intellectual"
    message_two = "Added Stan the Man"
    repo.add(test_table)
    repo.commit("Julianna, the very serious intellectual")
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Stan", 4)')
    repo.add(test_table)
    repo.commit(message_two)

    commits = list(repo.log(number=1).values())
    commits = list(repo.log(commit=commits[0].ref).values())

    assert len(commits) == 1
    current_commit = commits[0]
    assert current_commit.message == message_two


def test_dolt_log_merge_commit(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    message_one = "Base branch"
    message_two = "Base branch new data"
    message_three = "Other branch"
    message_merge = "merge"
    # commit the current working set to main
    repo.add(test_table)
    repo.commit(message_one)

    # create another branch from the working set
    repo.branch("other")

    # create a non-trivial commit against `main`
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Stan", 4)')
    repo.add(test_table)
    repo.commit(message_two)

    # create a non-trivial commit against `other`
    repo.checkout("other")
    repo.sql('INSERT INTO `test_players` (`name`, `id`) VALUES ("Juan Martin", 5)')
    repo.add(test_table)
    repo.commit(message_three)

    # merge
    repo.checkout("main")
    repo.merge("other", message_merge)

    commits = list(repo.log().values())
    merge_commit = commits[0]
    first_merge_parent = commits[1]
    second_merge_parent = commits[2]

    assert merge_commit.message == message_merge
    assert {first_merge_parent.ref, second_merge_parent.ref} == set(merge_commit.parents)


def test_get_dirty_tables(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    message = "Committing test data"

    # Some test data
    initial = [dict(id=1, name="Bianca", role="Champion")]
    appended_row = [dict(id=1, name="Serena", role="Runner-up")]

    def _insert_row_helper(repo, table, row):
        write_rows(repo, table, row, UPDATE, commit=False)

    # existing, not modified
    repo.add(test_table)
    repo.commit(message)

    # existing, modified, staged
    modified_staged = "modified_staged"
    write_rows(repo, modified_staged, initial, commit=False)
    repo.add(modified_staged)

    # existing, modified, unstaged
    modified_unstaged = "modified_unstaged"
    write_rows(repo, modified_unstaged, initial, commit=False)
    repo.add(modified_unstaged)

    # Commit and modify data
    repo.commit(message)
    _insert_row_helper(repo, modified_staged, appended_row)
    write_rows(repo, modified_staged, appended_row, UPDATE, commit=False)
    repo.add(modified_staged)
    write_rows(repo, modified_unstaged, appended_row, UPDATE, commit=False)

    # created, staged
    created_staged = "created_staged"
    write_rows(
        repo,
        created_staged,
        initial,
        import_mode=CREATE,
        primary_key=["id"],
        commit=False,
    )
    repo.add(created_staged)

    # created, unstaged
    created_unstaged = "created_unstaged"
    write_rows(
        repo,
        created_unstaged,
        initial,
        import_mode=CREATE,
        primary_key=["id"],
        commit=False,
    )

    status = repo.status()

    expected_new_tables = {"created_staged": True, "created_unstaged": False}
    expected_changes = {"modified_staged": True, "modified_unstaged": False}

    assert status.added_tables == expected_new_tables
    assert status.modified_tables == expected_changes


def test_checkout_with_tables(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    repo.checkout(tables=test_table)
    assert repo.status().is_clean


def test_branch(create_test_table: Tuple[Dolt, str]):
    repo, _ = create_test_table
    active_branch, branches = repo.branch()
    assert [active_branch.name] == [branch.name for branch in branches] == ["main"]

    repo.checkout("dosac", checkout_branch=True)
    repo.checkout("main")
    next_active_branch, next_branches = repo.branch()
    assert (
        set(branch.name for branch in next_branches) == {"main", "dosac"}
        and next_active_branch.name == "main"
    )

    repo.checkout("dosac")
    different_active_branch, _ = repo.branch()
    assert different_active_branch.name == "dosac"


# we want to make sure that we can delte a branch atomically
def test_branch_delete(create_test_table: Tuple[Dolt, str]):
    repo, _ = create_test_table

    _verify_branches(repo, ["main"])

    repo.checkout("dosac", checkout_branch=True)
    repo.checkout("main")
    _verify_branches(repo, ["main", "dosac"])

    repo.branch("dosac", delete=True)
    _verify_branches(repo, ["main"])


def test_branch_move(create_test_table: Tuple[Dolt, str]):
    repo, _ = create_test_table

    _verify_branches(repo, ["main"])

    repo.branch("main", move=True, new_branch="dosac")
    _verify_branches(repo, ["dosac"])


def _verify_branches(repo: Dolt, branch_list: List[str]):
    _, branches = repo.branch()
    assert set(branch.name for branch in branches) == set(branch for branch in branch_list)


def test_remote_list(create_test_table: Tuple[Dolt, str]):
    repo, _ = create_test_table
    repo.remote(add=True, name="origin", url="blah-blah")
    assert repo.remote()[0].name == "origin"
    repo.remote(add=True, name="another-origin", url="blah-blah")
    assert set([remote.name for remote in repo.remote()]) == {
        "origin",
        "another-origin",
    }


def test_checkout_non_existent_branch(doltdb):
    repo = Dolt(doltdb)
    repo.checkout("main")


def test_ls(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    assert [table.name for table in repo.ls()] == [test_table]


def test_ls_empty(init_empty_test_repo: Dolt):
    repo = init_empty_test_repo
    assert len(repo.ls()) == 0


def test_sql(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    sql = """
        INSERT INTO {table} (name, id)
        VALUES ('Roger', 3)
    """.format(
        table=test_table
    )
    repo.sql(query=sql)

    test_data = read_rows(repo, test_table)
    assert "Roger" in [x["name"] for x in test_data]


def test_sql_json(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    result = repo.sql(
        query="SELECT * FROM `{table}`".format(table=test_table), result_format="json"
    )["rows"]
    _verify_against_base_rows(result)


def test_sql_csv(create_test_table: Tuple[Dolt, str]):
    repo, test_table = create_test_table
    result = repo.sql(query="SELECT * FROM `{table}`".format(table=test_table), result_format="csv")
    _verify_against_base_rows(result)


def _verify_against_base_rows(result: List[dict]):
    assert len(result) == len(BASE_TEST_ROWS)

    result_sorted = sorted(result, key=lambda el: el["id"])
    for left, right in zip(BASE_TEST_ROWS, result_sorted):
        assert set(left.keys()) == set(right.keys())
        for k in left.keys():
            # Unfortunately csv.DictReader is a stream reader and thus does not look at all values for a given column
            # and make type inference, so we have to cast everything to a string. JSON round-trips, but would not
            # preserve datetime objects for example.
            assert str(left[k]) == str(right[k])


TEST_IMPORT_FILE_DATA = """
name,id
roger,1
rafa,2
""".lstrip()


def test_schema_import_create(init_empty_test_repo: Dolt, tmp_path):
    repo = init_empty_test_repo
    table = "test_table"
    test_file = tmp_path / "test_data.csv"
    with open(test_file, "w") as f:
        f.writelines(TEST_IMPORT_FILE_DATA)
    repo.schema_import(table=table, create=True, pks=["id"], filename=test_file)

    assert repo.status().added_tables == {table: False}


def test_config_global(init_empty_test_repo: Dolt):
    _ = init_empty_test_repo
    current_global_config = Dolt.config_global(list=True)
    test_username, test_email = "test_user", "test_email"
    Dolt.config_global(add=True, name="user.name", value=test_username)
    Dolt.config_global(add=True, name="user.email", value=test_email)
    updated_config = Dolt.config_global(list=True)
    assert (
        updated_config["user.name"] == test_username and updated_config["user.email"] == test_email
    )
    Dolt.config_global(add=True, name="user.name", value=current_global_config["user.name"])
    Dolt.config_global(add=True, name="user.email", value=current_global_config["user.email"])
    reset_config = Dolt.config_global(list=True)
    assert reset_config["user.name"] == current_global_config["user.name"]
    assert reset_config["user.email"] == current_global_config["user.email"]


def test_config_local(init_empty_test_repo: Dolt):
    repo = init_empty_test_repo
    current_global_config = Dolt.config_global(list=True)
    test_username, test_email = "test_user", "test_email"
    repo.config_local(add=True, name="user.name", value=test_username)
    repo.config_local(add=True, name="user.email", value=test_email)
    local_config = repo.config_local(list=True)
    global_config = Dolt.config_global(list=True)
    assert local_config["user.name"] == test_username and local_config["user.email"] == test_email
    assert global_config["user.name"] == current_global_config["user.name"]
    assert global_config["user.email"] == current_global_config["user.email"]


def test_detached_head_cm(doltdb):
    db = Dolt(doltdb)
    commits = list(db.log().keys())

    with detach_head(db, commits[1]):
        sum1 = db.sql("select sum(a) as sum from t1", result_format="csv")[0]

    with detach_head(db, commits[0]):
        sum2 = db.sql("select sum(a) as sum from t1", result_format="csv")[0]

    assert sum1["sum"] == "3"
    assert sum2["sum"] == "6"


def test_get_clone_dir_no_remote(tmp_path):
    new_dir = os.path.join(tmp_path, "new_dir")
    res = Dolt._get_clone_dir(new_dir)
    assert new_dir == res


def test_get_clone_dir_remote_only(tmp_path):
    new_dir = os.path.join(os.getcwd(), "remote")
    res = Dolt._get_clone_dir(remote_url="some/remote")
    assert new_dir == res


def test_get_clone_dir_new_dir_only(tmp_path):
    res = Dolt._get_clone_dir("new_dir")
    assert "new_dir" == res


def test_get_clone_dir_new_dir_and_remote(tmp_path):
    new_dir = os.path.join("foo/bar", "remote")
    res = Dolt._get_clone_dir(new_dir="foo/bar", remote_url="some/remote")
    assert new_dir == res


def test_clone_new_dir(tmp_path):
    target = os.path.join(tmp_path, "state_age")
    Dolt.clone("max-hoffman/state-age", new_dir=target)
    db = Dolt(target)
    assert db.head is not None


def test_dolt_sql_csv(init_empty_test_repo: Dolt):
    dolt = init_empty_test_repo
    write_rows(dolt, "test_table", BASE_TEST_ROWS, commit=True)
    result = dolt.sql(
        "SELECT `name` as name, `id` as id FROM test_table ORDER BY id", result_format="csv"
    )
    assert BASE_TEST_ROWS == result


def test_dolt_sql_json(init_empty_test_repo: Dolt):
    dolt = init_empty_test_repo
    write_rows(dolt, "test_table", BASE_TEST_ROWS, commit=True)
    result = dolt.sql("SELECT `name` as name, `id` as id FROM test_table ", result_format="json")
    # JSON return value preserves some type information, we cast back to a string
    for row in result["rows"]:
        row["id"] = str(row["id"])
    compare_rows_helper(BASE_TEST_ROWS, result["rows"])


def test_dolt_sql_file(init_empty_test_repo: Dolt):
    dolt = init_empty_test_repo

    with tempfile.NamedTemporaryFile() as f:
        write_rows(dolt, "test_table", BASE_TEST_ROWS, commit=True)
        _ = dolt.sql("SELECT `name` as name, `id` as id FROM test_table ", result_file=f.name)
        res = read_csv_to_dict(f.name)
        compare_rows_helper(BASE_TEST_ROWS, res)


def test_dolt_sql_errors(doltdb):
    db = Dolt(doltdb)

    with pytest.raises(ValueError):
        db.sql(result_parser=lambda x: x, query=None)
    with pytest.raises(ValueError):
        db.sql(result_parser=2, query="select active_branch()")
    with pytest.raises(ValueError):
        db.sql(result_file="file.csv", query=None)
    with pytest.raises(ValueError):
        db.sql(result_format="csv", query=None)


def test_no_init_error(init_empty_test_repo: Dolt):
    dolt = init_empty_test_repo

    dolt.init(dolt.repo_dir, error=False)


def test_set_dolt_path_error(doltdb):
    db = Dolt(doltdb)
    set_dolt_path("dolt")
    test_cmd = "show tables"
    db.sql(test_cmd, result_format="csv")
    try:
        with pytest.raises(FileNotFoundError):
            set_dolt_path("notdolt")
            from doltcli.utils import DOLT_PATH

            assert DOLT_PATH == "notdolt"
            db.sql(test_cmd, result_format="csv")
    finally:
        set_dolt_path("dolt")


def test_no_checkout_error(init_empty_test_repo: Dolt):
    dolt = init_empty_test_repo

    dolt.checkout(branch="main", error=False)


def test_reset(doltdb):
    db = Dolt(doltdb)
    db.reset()
    db.reset(hard=True)
    db.reset(soft=True)
    db.reset(tables="t1")
    db.reset(tables=["t1"])


def test_reset_errors(doltdb):
    db = Dolt(doltdb)
    with pytest.raises(ValueError):
        db.reset(hard=True, soft=True)
    with pytest.raises(ValueError):
        db.reset(tables="t1", hard=True)
    with pytest.raises(ValueError):
        db.reset(tables="t1", soft=True)
    with pytest.raises(ValueError):
        db.reset(tables={"t1": True})


def test_repo_name_trailing_slash(tmp_path):
    repo_path, repo_data_dir = get_repo_path_tmp_path(tmp_path)
    assert Dolt.init(str(repo_path) + "/").repo_name == "test_repo_name_trailing_slash0"
    shutil.rmtree(repo_data_dir)
