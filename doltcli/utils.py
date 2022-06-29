import csv
import datetime
import io
import logging
import os
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Union

from .types import DoltT

logger = logging.getLogger()

DOLT_PATH = "dolt"


def set_dolt_path(path: str):
    global DOLT_PATH
    DOLT_PATH = path


def read_columns(dolt: DoltT, table: str, as_of: Optional[str] = None) -> Dict[str, list]:
    return read_columns_sql(dolt, get_read_table_asof_query(table, as_of))


def read_rows(dolt: DoltT, table: str, as_of: Optional[str] = None) -> List[dict]:
    return read_rows_sql(dolt, get_read_table_asof_query(table, as_of))


def get_read_table_asof_query(table: str, as_of: Optional[str] = None) -> str:
    base_query = f"SELECT * FROM `{table}`"
    return f'{base_query} AS OF "{as_of}"' if as_of else base_query


def read_columns_sql(dolt: DoltT, sql: str) -> Dict[str, list]:
    rows = read_table_sql(dolt, sql)
    columns = rows_to_columns(rows)
    return columns


def read_rows_sql(dolt: DoltT, sql: str) -> List[dict]:
    return read_table_sql(dolt, sql)


def read_table_sql(
    dolt: DoltT, sql: str, result_parser: Optional[Callable[[str], Any]] = None
) -> List[dict]:
    return dolt.sql(sql, result_format="csv", result_parser=result_parser)


CREATE, FORCE_CREATE, REPLACE, UPDATE = "create", "force_create", "replace", "update"
IMPORT_MODES_TO_FLAGS = {
    CREATE: ["-c"],
    FORCE_CREATE: ["-f", "-c"],
    REPLACE: ["-r"],
    UPDATE: ["-u"],
}


def write_file(
    dolt: DoltT,
    table: str,
    file_handle: Optional[io.TextIOBase] = None,
    file: Union[str, Path, None] = None,
    # TODO what to do about this?
    filetype: str = "csv",
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
    do_continue: Optional[bool] = False,
):
    if file_handle is not None and file is not None:
        raise ValueError("Specify one of: file, file_handle")
    elif file_handle is None and file is None:
        raise ValueError("Specify one of: file, file_handle")
    elif file_handle is not None:

        def writer(filepath: str):
            if not isinstance(file_handle, io.TextIOBase):
                raise ValueError(
                    f"file_handle expected type io.StringIO; found: {type(file_handle)}"
                )
            with open(filepath, "w", newline="") as f:
                f.writelines(file_handle.readlines())
            return filepath

    elif file is not None:

        def writer(filepath: str):
            return str(file)

    _import_helper(
        dolt=dolt,
        table=table,
        write_import_file=writer,
        primary_key=primary_key,
        import_mode=import_mode,
        commit=commit,
        commit_message=commit_message,
        commit_date=commit_date,
        do_continue=do_continue,
    )


def write_columns(
    dolt: DoltT,
    table: str,
    columns: Dict[str, List[Any]],
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
    do_continue: Optional[bool] = False,
):
    """

    :param dolt:
    :param table:
    :param columns:
    :param import_mode:
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :return:
    """

    def writer(filepath: str):
        if len(list(set(len(col) for col in columns.values()))) != 1:
            raise ValueError("Must pass columns of identical length")

        with open(filepath, "w", newline="") as f:
            csv_writer = csv.DictWriter(f, columns.keys())
            rows = columns_to_rows(columns)
            csv_writer.writeheader()
            csv_writer.writerows(rows)
        return filepath

    _import_helper(
        dolt=dolt,
        table=table,
        write_import_file=writer,
        primary_key=primary_key,
        import_mode=import_mode,
        commit=commit,
        commit_message=commit_message,
        commit_date=commit_date,
        do_continue=do_continue,
    )


def write_rows(
    dolt: DoltT,
    table: str,
    rows: List[dict],
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
    do_continue: Optional[bool] = False,
):
    """

    :param dolt:
    :param table:
    :param rows:
    :param import_mode:
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :return:
    """

    def writer(filepath: str):
        with open(filepath, "w", newline="") as f:
            fieldnames: Set[str] = set()
            for row in rows:
                fieldnames = fieldnames.union(set(row.keys()))

            csv_writer = csv.DictWriter(f, fieldnames)
            csv_writer.writeheader()
            csv_writer.writerows(rows)
        return filepath

    _import_helper(
        dolt=dolt,
        table=table,
        write_import_file=writer,
        primary_key=primary_key,
        import_mode=import_mode,
        commit=commit,
        commit_message=commit_message,
        commit_date=commit_date,
        do_continue=do_continue,
    )


def _import_helper(
    dolt: DoltT,
    table: str,
    write_import_file: Callable[[str], str],
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    do_continue: Optional[bool] = False,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
) -> None:
    import_mode = _get_import_mode_and_flags(dolt, table, import_mode)
    logger.info(
        f"Importing to table {table} in dolt directory located in {dolt.repo_dir}, import mode {import_mode}"
    )

    fname = tempfile.mktemp(suffix=".csv")
    import_flags = IMPORT_MODES_TO_FLAGS[import_mode]
    try:
        import_file = write_import_file(fname)
        args = ["table", "import", table] + import_flags
        if primary_key:
            args += ["--pk={}".format(",".join(primary_key))]
        if do_continue is True:
            args += ["--continue"]

        dolt.execute(args + [import_file])

        if commit:
            msg = commit_message or f"Committing write to table {table} in {import_mode} mode"
            dolt.add(table)
            dolt.commit(msg, date=commit_date)
    finally:
        if os.path.exists(fname):
            os.remove(fname)


def _get_import_mode_and_flags(
    dolt: DoltT, table: str, import_mode: Optional[str] = None
) -> str:
    import_modes = IMPORT_MODES_TO_FLAGS.keys()
    if import_mode and import_mode not in import_modes:
        raise ValueError(f"update_mode must be one of: {import_modes}")
    elif not import_mode:
        if table in [table.name for table in dolt.ls()]:
            logger.info(f'No import mode specified, table exists, using "{UPDATE}"')
            import_mode = UPDATE
        else:
            logger.info(f'No import mode specified, table does not exist, using "{CREATE}"')
            import_mode = CREATE

    return import_mode


def columns_to_rows(columns: Dict[str, list]) -> List[dict]:
    row_count = len(list(columns.values())[0])
    rows: List[dict] = [{} for _ in range(row_count)]
    for col_name in columns.keys():
        for j, val in enumerate(columns[col_name]):
            rows[j][col_name] = val

    return rows


def rows_to_columns(rows: Iterable[dict]) -> Dict[str, list]:
    columns: Dict[str, list] = defaultdict(list)
    for i, row in enumerate(list(rows)):
        for col, val in row.items():
            columns[col].append(val)

    return columns


def to_list(value: Union[Any, List[Any]]) -> Any:
    return [value] if not isinstance(value, list) and value is not None else value


@contextmanager
def detach_head(db, commit):
    active_branch, _ = db._get_branches()
    switched = False
    try:
        commit_branches = db.sql(
            f"select name, hash from dolt_branches where hash = '{commit}'",
            result_format="csv",
        )
        if len(commit_branches) > 0:
            tmp_branch = commit_branches[0]
            if active_branch.hash != tmp_branch["hash"]:
                switched = True
                db.checkout(tmp_branch["name"])
        else:
            tmp_branch = f"detached_HEAD_at_{commit[:5]}"
            db.checkout(start_point=commit, branch=tmp_branch, checkout_branch=True)
            switched = True
        yield
    finally:
        if switched:
            db.checkout(active_branch.name)
        return
