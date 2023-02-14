import csv
import datetime
import json
import logging
import os
import shutil
import tempfile
from collections import OrderedDict
from subprocess import PIPE, Popen
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .types import BranchT, CommitT, DoltT, KeyPairT, RemoteT, StatusT, TableT
from .utils import (
    read_columns,
    read_columns_sql,
    read_rows,
    read_rows_sql,
    to_list,
    write_columns,
    write_file,
    write_rows,
)

global logger
logger = logging.getLogger(__name__)


SQL_OUTPUT_PARSERS = {
    "csv": lambda fh: list(csv.DictReader(fh)),
    "json": lambda fh: json.load(fh),
}


class DoltException(Exception):

    """
    A class representing a Dolt exception.
    """

    def __init__(
        self,
        exec_args,
        stdout: Optional[Union[str, bytes]] = None,
        stderr: Optional[Union[str, bytes]] = None,
        exitcode: Optional[int] = 1,
    ):
        super().__init__(exec_args, stdout, stderr, exitcode)
        self.exec_args = exec_args
        self.stdout = stdout
        self.stderr = stderr
        self.exitcode = exitcode


class DoltServerNotRunningException(Exception):
    def __init__(self, message):
        self.message = message


class DoltWrongServerException(Exception):
    def __init__(self, message):
        self.message = message


class DoltDirectoryException(Exception):
    def __init__(self, message):
        self.message = message


def _execute(args: List[str], cwd: Optional[str] = None, outfile: Optional[str] = None):
    from .utils import DOLT_PATH

    _args = [DOLT_PATH] + args
    str_args = " ".join(" ".join(args).split())
    logger.info(str_args)
    if outfile:
        with open(outfile, "w", newline="") as f:
            proc = Popen(args=_args, cwd=cwd, stdout=f, stderr=PIPE)
    else:
        proc = Popen(args=_args, cwd=cwd, stdout=PIPE, stderr=PIPE)
    out, err = (val.decode("utf8") if val else "" for val in proc.communicate())
    exitcode = proc.returncode

    if exitcode != 0:
        logger.error(err)
        raise DoltException(str_args, out, err, exitcode)

    if outfile:
        return outfile
    else:
        return out


class Status(StatusT):
    """
    Represents the current status of a Dolt repo, summarized by the is_clean field which is True if the wokring set is
    clean, and false otherwise. If the working set is not clean, then the changes are stored in maps, one for added
    tables, and one for modifications, each name maps to a flag indicating whether the change is staged.
    """

    pass


class Table(TableT):
    """
    Represents a Dolt table in the working set.
    """

    def __str__(self):
        return f"Table(name: {self.name}, table_hash: {self.table_hash}, rows: {self.rows}, system: {self.system})"


class Commit(CommitT):
    """
    Represents metadata about a commit, including a ref, timestamp, and author, to make it easier to sort and present
    to the user.
    """

    def __str__(self):
        return f"{self.ref}: {self.author} @ {self.timestamp}, {self.message}"

    def is_merge(self):
        return isinstance(self.parents, tuple)

    def append_parent(self, parent: str):
        if isinstance(self.parents, tuple):
            raise ValueError("Already has a merge parent set")
        elif isinstance(self.parents, str):
            self.parents = [self.parents, parent]
            self.merge = True
        elif not self.parents:
            logger.warning("No merge parents set")
            return

    @classmethod
    def get_log_table_query(
        cls,
        number: Optional[int] = None,
        commit: Optional[str] = None,
        head: Optional[str] = None,
    ):
        base = """
            select
                dc.`commit_hash` as commit_hash,
                dca.`parent_hash` as parent_hash,
                `committer` as committer,
                `email` as email,
                `date` as date,
                `message` as message
            from
                dolt_log as dc
                left outer join dolt_commit_ancestors as dca
                    on dc.commit_hash = dca.commit_hash
        """

        if commit is not None:
            base += f"\nWHERE dc.`commit_hash`='{commit}'"

        base += "\nORDER BY `date` DESC"

        if number is not None:
            base += f"\nLIMIT {number}"

        return base

    @classmethod
    def parse_dolt_log_table(cls, rows: List[dict]) -> Dict:
        commits: Dict[str, Commit] = OrderedDict()
        for row in rows:
            ref = row["commit_hash"]
            if ref in commits:
                commits[ref].append_parent(row["parent_hash"])
            else:
                commit = Commit(
                    ref=row["commit_hash"],
                    timestamp=row["date"],
                    author=row["committer"],
                    email=row["email"],
                    message=row["message"],
                    parents=row["parent_hash"],
                    merge=False,
                )
                commits[ref] = commit

        return commits


class KeyPair(KeyPairT):
    """
    Represents a key pair generated by Dolt for authentication with remotes.
    """

    def __init__(self, public_key: str, key_id: str, active: bool):
        self.public_key = public_key
        self.key_id = key_id
        self.active = active


class Branch(BranchT):
    """
    Represents a branch, along with the commit it points to.
    """

    def __str__(self):
        return f"branch name: {self.name}, hash:{self.hash}"


class Remote(RemoteT):
    """
    Represents a remote, effectively a name and URL pair.
    """

    pass


class DoltHubContext:
    def __init__(
        self,
        db_path: str,
        path: Optional[str] = None,
        remote: str = "origin",
        tables_to_read: Optional[List[str]] = None,
    ):
        self.db_path = db_path
        self.path = (
            os.path.join(tempfile.mkdtemp(), self._get_db_name(db_path)) if not path else path
        )
        self.remote = remote
        self.dolt = None
        self.tables_to_read = tables_to_read

    @classmethod
    def _get_db_name(cls, db_path):
        split = db_path.split("/")
        if len(split) != 2:
            raise ValueError(f"Invalid DoltHub path {db_path}")
        return split[1]

    def __enter__(self):
        try:
            dolt = Dolt(self.path)
            logger.info(
                f'Dolt database found at path provided ({self.path}), pulling from remote "{self.remote}"'
            )
            dolt.pull(self.remote)
        except ValueError:
            if self.db_path is None:
                raise ValueError("Cannot clone remote data without db_path set")
            if self.tables_to_read:
                logger.info(f"Running read-tables, creating a fresh copy of {self.db_path}")
                dolt = Dolt.read_tables(self.db_path, "master", tables=self.tables_to_read)
            else:
                logger.info(f"Running clone, cloning remote {self.db_path}")
                dolt = Dolt.clone(self.db_path, self.path)

        self.dolt = dolt
        return self

    def __exit__(self, type, value, traceback):
        pass


class Dolt(DoltT):
    """
    This class wraps the Dolt command line interface, mimicking functionality exactly to the extent that is possible.
    Some commands simply do not translate to Python, such as `dolt sql` (with no arguments) since that command
    launches an interactive shell.
    """

    def __init__(self, repo_dir: str, print_output: Optional[bool] = None):
        self.repo_dir = repo_dir
        self._print_output = print_output or False

        if not os.path.exists(os.path.join(self.repo_dir, ".dolt")):
            raise ValueError(f"{self.repo_dir} is not a valid Dolt repository")

    @property
    def repo_name(self):
        return os.path.basename(os.path.normpath(self.repo_dir)).replace("-", "_")

    @property
    def head(self):
        head_hash = "HASHOF('HEAD')"
        head_commit = self.sql(f"select {head_hash} as hash", result_format="csv")[0].get(
            "hash", None
        )
        if not head_commit:
            raise ValueError("Head not found")
        return head_commit

    @property
    def working(self):
        working = self.sql(
            f"select @@{self.repo_name}_working as working", result_format="csv"
        )[0].get("working", None)
        if not working:
            raise ValueError("Working head not found")
        return working

    @property
    def active_branch(self):
        active_branch = self.sql("select active_branch() as a", result_format="csv")[0].get(
            "a", None
        )
        if not active_branch:
            raise ValueError("Active branch not found")
        return active_branch

    def execute(
        self,
        args: List[str],
        print_output: Optional[bool] = None,
        stdout_to_file: str = None,
        error: bool = True,
    ) -> str:
        """
        Manages executing a dolt command, pass all commands, sub-commands, and arguments as they would appear on the
        command line.
        :param args:
        :param print_output:
        :param stdout_to_file:
        :return:
        """
        if print_output and stdout_to_file is not None:
            raise ValueError("Cannot print output and send it to a file")

        if not error:
            try:
                output = _execute(args, self.repo_dir, outfile=stdout_to_file)
            except DoltException as e:
                output = repr(e)
        else:
            output = _execute(args, self.repo_dir, outfile=stdout_to_file)

        print_output = print_output or self._print_output
        if print_output:
            logger.info(output)

        if stdout_to_file:
            return stdout_to_file
        else:
            return output

    @staticmethod
    def init(repo_dir: Optional[str] = None, error: bool = False) -> "Dolt":
        """
        Creates a new repository in the directory specified, creating the directory if `create_dir` is passed, and returns
        a `Dolt` object representing the newly created repo.
        :return:
        """
        if not repo_dir:
            repo_dir = os.getcwd()

        os.makedirs(repo_dir, exist_ok=True)
        logger.info(f"Initializing Dolt repo in {repo_dir}")

        try:
            _execute(["init"], cwd=repo_dir)
        except DoltException:
            if not error:
                return Dolt(repo_dir)
        return Dolt(repo_dir)

    @staticmethod
    def version():
        return _execute(["version"], cwd=os.getcwd()).split(" ")[2].strip()

    def status(self, **kwargs) -> Status:
        """
        Parses the status of this repository into a `Status` object.
        :return:
        """
        new_tables: Dict[str, bool] = {}
        changes: Dict[str, bool] = {}

        output = self.execute(["status"], print_output=False, **kwargs).split("\n")

        if "clean" in str("\n".join(output)):
            return Status(True, changes, new_tables)
        else:
            staged = False
            for line in output:
                _line = line.lstrip()
                if _line.startswith("Changes to be committed"):
                    staged = True
                elif _line.startswith("Changes not staged for commit"):
                    staged = False
                elif _line.startswith("Untracked files"):
                    staged = False
                elif _line.startswith("modified"):
                    changes[_line.split(":")[1].lstrip()] = staged
                elif _line.startswith("new table"):
                    new_tables[_line.split(":")[1].lstrip()] = staged
                else:
                    pass

        return Status(False, changes, new_tables)

    def add(self, tables: Union[str, List[str]], **kwargs) -> Status:
        """
        Adds the table or list of tables in the working tree to staging.
        :param tables:
        :return:
        """
        self.execute(["add"] + to_list(tables), **kwargs)
        return self.status()

    def reset(
        self,
        tables: Union[str, List[str]] = [],
        revision: str = "",
        hard: bool = False,
        soft: bool = False,
        **kwargs,
    ):
        """
        Reset a table or set of tables that have changes in the working set to their value at the tip of the current
        branch.
        :param tables:
        :param hard:
        :param soft:
        :return:
        """
        if not isinstance(tables, (str, list)):
            raise ValueError(f"tables should be: Union[str, List[str]]; found {type(tables)}")

        to_reset = to_list(tables)

        args = ["reset"]

        if hard and soft:
            raise ValueError("Specify one of: hard=True, soft=True")

        if (hard or soft) and to_reset:
            raise ValueError("Specify either hard/soft flag, or tables to reset")

        if to_reset and revision != "":
            raise ValueError("Specify either revision or tables to reset")

        if revision != "":
            args.append(revision)

        if hard:
            args.append("--hard")
        elif soft:
            args.append("--soft")
        elif not tables:
            args.append("--soft")
        else:
            args += to_reset

        self.execute(args, **kwargs)

    def commit(
        self,
        message: Optional[str] = None,
        allow_empty: bool = False,
        date: datetime.datetime = None,
        **kwargs,
    ):
        """
        Create a commit with the currents in the working set that are currently in staging.
        :param message:
        :param allow_empty:
        :param date:
        :return:
        """
        if message is None:
            message = ""

        args = ["commit", "-m", message]

        if allow_empty:
            args.append("--allow-empty")

        if date:
            # TODO format properly
            args.extend(["--date", str(date)])

        self.execute(args, **kwargs)

    def merge(
        self, branch: str, message: Optional[str] = None, squash: bool = False, **kwargs
    ):
        """
        Executes a merge operation. If conflicts result, the merge is aborted, as an interactive merge does not really
        make sense in a scripting environment, or at least we have not figured out how to model it in a way that does.
        :param branch:
        :param message:
        :param squash:
        :return:
        """
        current_branch, branches = self._get_branches()
        if not self.status().is_clean:
            err = f"Changes in the working set, please commit before merging {branch} to {current_branch.name}"
            raise ValueError(err)
        if branch not in [branch.name for branch in branches]:
            raise ValueError(
                f"Trying to merge in non-existent branch {branch} to {current_branch.name}"
            )

        logger.info(f"Merging {branch} into {current_branch.name}")
        args = ["merge"]

        if squash:
            args.append("--squash")

        args.append(branch)
        output = self.execute(args, **kwargs).split("\n")
        merge_conflict_pos = 2

        if len(output) == 3 and "Fast-forward" in output[1]:
            logger.info(f"Completed fast-forward merge of {branch} into {current_branch.name}")
            return

        if len(output) == 5 and output[merge_conflict_pos].startswith("CONFLICT"):
            logger.warning(
                f"""
                The following merge conflict occurred merging {branch} to {current_branch.name}:
                {output[merge_conflict_pos]}
            """
            )
            logger.warning("Aborting as interactive merge not supported in Doltpy")
            abort_args = ["merge", "--abort"]
            self.execute(abort_args)
            return

        if message is None:
            message = f"Merged {current_branch.name} into {branch}"
        logger.info(message)
        status = self.status()

        for table in list(status.added_tables.keys()) + list(status.modified_tables.keys()):
            self.add(table)

        self.commit(message)

    def sql(
        self,
        query: Optional[str] = None,
        result_format: Optional[str] = None,
        execute: bool = False,
        save: Optional[str] = None,
        message: Optional[str] = None,
        list_saved: bool = False,
        batch: bool = False,
        multi_db_dir: Optional[str] = None,
        result_file: Optional[str] = None,
        result_parser: Optional[Callable[[str], Any]] = None,
        **kwargs,
    ):
        """
        Execute a SQL query, using the options to dictate how it is executed, and where the output goes.
        :param query: query to be executed
        :param result_format: the file format of the
        :param execute: execute a saved query, not valid with other parameters
        :param save: use the name provided to save the value of query
        :param message: the message associated with the saved query, if any
        :param list_saved: print out a list of saved queries
        :param batch: execute in batch mode, one statement after the other delimited by ;
        :param multi_db_dir: use a directory of Dolt repos, each one treated as a database
        :param result_parser:
        :return:
        """
        args = ["sql"]

        if list_saved:
            if any([query, result_format, save, message, batch, multi_db_dir]):
                raise ValueError("Incompatible arguments provided")
            args.append("--list-saved")
            self.execute(args, **kwargs)

        if execute:
            if any([query, save, message, list_saved, batch, multi_db_dir]):
                raise ValueError("Incompatible arguments provided")
            args.extend(["--execute", str(execute)])

        if multi_db_dir:
            args.extend(["--multi-db-dir", multi_db_dir])

        if batch:
            args.append("--batch")

        if save:
            args.extend(["--save", save])
            if message:
                args.extend(["--message", message])

        # do something with result format
        if result_parser is not None:
            if query is None:
                raise ValueError("Must provide a query in order to specify a result format")
            args.extend(["--query", query])

            try:
                d = tempfile.mkdtemp()
                args.extend(["--result-format", "csv"])
                f = os.path.join(d, "tmpfile")
                output_file = self.execute(args, stdout_to_file=f, **kwargs)
                if not hasattr(result_parser, "__call__"):
                    raise ValueError(
                        f"Invalid argument: `result_parser` should be Callable; found {type(result_parser)}"
                    )
                return result_parser(output_file)
            finally:
                shutil.rmtree(d, ignore_errors=True, onerror=None)
        elif result_file is not None:
            if query is None:
                raise ValueError("Must provide a query in order to specify a result format")
            args.extend(["--query", query])

            args.extend(["--result-format", "csv"])
            output_file = self.execute(args, stdout_to_file=result_file, **kwargs)
            return output_file
        elif result_format in ["csv", "json"]:
            if query is None:
                raise ValueError("Must provide a query in order to specify a result format")
            args.extend(["--query", query])

            try:
                d = tempfile.mkdtemp()
                f = os.path.join(d, "tmpfile")
                args.extend(["--result-format", result_format])
                output_file = self.execute(args, stdout_to_file=f, **kwargs)
                with open(output_file, newline="") as fh:
                    return SQL_OUTPUT_PARSERS[result_format](fh)
            finally:
                shutil.rmtree(d, ignore_errors=True, onerror=None)

        logger.warning("Must provide a value for result_format to get output back")
        if query is not None:
            args.extend(["--query", query])

        self.execute(args, **kwargs)

    def log(self, number: Optional[int] = None, commit: Optional[str] = None) -> Dict:
        """
        Parses the log created by running the log command into instances of `Commit` that provide detail of the
        commit, including timestamp and hash.
        :param number:
        :param commit:
        :return:
        """
        res = read_rows_sql(
            self,
            sql=Commit.get_log_table_query(number=number, commit=commit, head=self.head),
        )
        commits = Commit.parse_dolt_log_table(res)
        return commits

    def diff(
        self,
        commit: Optional[str] = None,
        other_commit: Optional[str] = None,
        tables: Optional[Union[str, List[str]]] = None,
        data: bool = False,
        schema: bool = False,  # can we even support this?
        summary: bool = False,
        sql: bool = False,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs,
    ):
        """
        Executes a diff command and prints the output. In the future we plan to create a diff object that will allow
        for programmatic interactions.
        :param commit: commit to diff against the tip of the current branch
        :param other_commit: optionally specify two specific commits if desired
        :param tables: table or list of tables to diff
        :param data: diff only data
        :param schema: diff only schema
        :param summary: summarize the data changes shown, valid only with data
        :param sql: show the diff in terms of SQL
        :param where: apply a where clause to data diffs
        :param limit: limit the number of rows shown in a data diff
        :return:
        """
        switch_count = [el for el in [data, schema, summary] if el]
        if len(switch_count) > 1:
            raise ValueError("At most one of delete, copy, move can be set to True")

        args = ["diff"]

        if data:
            if where:
                args.extend(["--where", where])
            if limit:
                args.extend(["--limit", str(limit)])

        if summary:
            args.append("--summary")

        if schema:
            args.extend("--schema")

        if sql:
            args.append("--sql")

        if commit:
            args.append(commit)
        if other_commit:
            args.append(other_commit)

        if tables:
            args.append(" ".join(to_list(tables)))

        self.execute(args, **kwargs)

    def blame(self, table_name: str, rev: Optional[str] = None, **kwargs):
        """
        Executes a blame command that prints out a table that shows the authorship of the last change to a row.
        :param table_name:
        :param rev:
        :return:
        """
        args = ["blame"]

        if rev:
            args.append(rev)

        args.append(table_name)
        self.execute(args, **kwargs)

    def branch(
        self,
        branch_name: Optional[str] = None,
        start_point: Optional[str] = None,
        new_branch: Optional[str] = None,
        force: bool = False,
        delete: bool = False,
        copy: bool = False,
        move: bool = False,
        **kwargs,
    ):
        """
        Checkout, create, delete, move, or copy, a branch. Only
        :param branch_name:
        :param start_point:
        :param new_branch:
        :param force:
        :param delete:
        :param copy:
        :param move:
        :return:
        """
        switch_count = [el for el in [delete, copy, move] if el]
        if len(switch_count) > 1:
            raise ValueError("At most one of delete, copy, move can be set to True")

        if not any([branch_name, delete, copy, move]):
            if force:
                raise ValueError(
                    "force is not valid without providing a new branch name, or copy, move, or delete being true"
                )
            return self._get_branches()

        args = ["branch"]
        if force:
            args.append("--force")

        def execute_wrapper(command_args: List[str]):
            self.execute(command_args, **kwargs)
            return self._get_branches()

        if branch_name and not (delete or copy or move):
            args.append(branch_name)
            if start_point:
                args.append(start_point)
            return execute_wrapper(args)

        if copy:
            if not new_branch:
                raise ValueError("must provide new_branch when copying a branch")
            args.append("--copy")
            if branch_name:
                args.append(branch_name)
            args.append(new_branch)
            return execute_wrapper(args)

        if delete:
            if not branch_name:
                raise ValueError("must provide branch_name when deleting")
            args.extend(["--delete", branch_name])
            return execute_wrapper(args)

        if move:
            if not new_branch:
                raise ValueError("must provide new_branch when moving a branch")
            args.append("--move")
            if branch_name:
                args.append(branch_name)
            args.append(new_branch)
            return execute_wrapper(args)

        if branch_name:
            args.append(branch_name)
            if start_point:
                args.append(start_point)
            return execute_wrapper(args)

        return self._get_branches()

    def _get_branches(self) -> Tuple[Branch, List[Branch]]:
        dicts = read_rows_sql(self, sql="select * from dolt_branches")
        branches = [Branch(**d) for d in dicts]
        ab_dicts = read_rows_sql(
            self, "select * from dolt_branches where name = (select active_branch())"
        )

        if len(ab_dicts) != 1:
            raise ValueError(
                "Ensure you have the latest version of Dolt installed, this is fixed as of 0.24.2"
            )

        active_branch = Branch(**ab_dicts[0])

        if not active_branch:
            raise DoltException("Failed to set active branch")

        return active_branch, branches

    def checkout(
        self,
        branch: Optional[str] = None,
        tables: Optional[Union[str, List[str]]] = None,
        checkout_branch: bool = False,
        start_point: Optional[str] = None,
        **kwargs,
    ):
        """
        Checkout an existing branch, or create a new one, optionally at a specified commit. Or, checkout a table or list
        of tables.
        :param branch: branch to checkout or create
        :param tables: table or tables to checkout
        :param checkout_branch: branch to checkout
        :param start_point: tip of new branch
        :return:
        """
        if tables and branch:
            raise ValueError("No tables may be provided when creating a branch with checkout")
        args = ["checkout"]

        if branch:
            if checkout_branch:
                args.append("-b")
            args.append(branch)
            if start_point:
                args.append(start_point)

        if tables:
            args.append(" ".join(to_list(tables)))

        self.execute(args, **kwargs)

    def remote(
        self,
        add: bool = False,
        name: Optional[str] = None,
        url: Optional[str] = None,
        remove: bool = None,
        **kwargs,
    ):
        """
        Add or remove remotes to this repository. Note we do not currently support some more esoteric options for using
        AWS and GCP backends, but will do so in a future release.
        :param add:
        :param name:
        :param url:
        :param remove:
        :return:
        """
        args = ["remote", "--verbose"]

        if not (add or remove):
            output = self.execute(args, print_output=False, **kwargs).split("\n")

            remotes = []
            for line in output:
                if not line:
                    break

                split = line.lstrip().split()
                remotes.append(Remote(split[0], split[1]))

            return remotes

        if remove:
            if add:
                raise ValueError("add and remove are not comptaibe ")
            if not name:
                raise ValueError("Must provide the name of a remote to move")
            args.extend(["remove", name])

        if add:
            if not (name and url):
                raise ValueError("Must provide name and url to add")
            args.extend(["add", name, url])

        self.execute(args, **kwargs)

    def push(
        self,
        remote: str,
        refspec: Optional[str] = None,
        set_upstream: bool = False,
        force: bool = False,
        **kwargs,
    ):
        """
        Push the branch to the specified remote. If set_upstream is provided will create an upstream reference of all branches
        in a repo.
        :param remote:
        :param refspec: optionally specify a branch to push
        :param set_upstream: add upstream reference for every branch successfully pushed
        :param force: overwrite the history of the upstream with this repo's history
        :return:
        """
        args = ["push"]

        if set_upstream:
            args.append("--set-upstream")

        if force:
            args.append("--force")

        args.append(remote)
        if refspec:
            args.append(refspec)

        # just print the output
        self.execute(args, **kwargs)

    def pull(self, remote: str = "origin", **kwargs):
        """
        Pull the latest changes from the specified remote.
        :param remote:
        :return:
        """
        self.execute(["pull", remote], **kwargs)

    def fetch(
        self,
        remote: str = "origin",
        refspecs: Union[str, List[str]] = None,
        force: bool = False,
        **kwargs,
    ):
        """
        Fetch the specified branch or list of branches from the remote provided, defaults to origin.
        :param remote: the reomte to fetch from
        :param refspecs: branch or branches to fetch
        :param force: whether to override local history with remote
        :return:
        """
        args = ["fetch"]

        if force:
            args.append("--force")
        if remote:
            args.append(remote)
        if refspecs:
            args.extend(to_list(refspecs))

        self.execute(args, **kwargs)

    @staticmethod
    def clone(
        remote_url: str,
        new_dir: Optional[str] = None,
        remote: Optional[str] = None,
        branch: Optional[str] = None,
        **kwargs,
    ) -> "Dolt":
        """
        Clones the specified DoltHub database into a new directory, or optionally an existing directory provided by the
        user.
        :param remote_url:
        :param new_dir:
        :param remote:
        :param branch:
        :return:
        """
        args = ["clone", remote_url]

        if remote:
            args.extend(["--remote", remote])

        if branch:
            args.extend(["--branch", branch])

        clone_dir = Dolt._get_clone_dir(new_dir, None if new_dir else remote_url)
        if not clone_dir:
            raise ValueError("Unable to infer new_dir")

        args.append(clone_dir)

        _execute(args, **kwargs)

        return Dolt(clone_dir)

    @classmethod
    def _get_clone_dir(
        cls, new_dir: Optional[str] = None, remote_url: Optional[str] = None
    ) -> str:
        """
        Takes either a new_dir to clone the
        """
        if not (new_dir or remote_url):
            raise ValueError("Provide either new_dir or remote_url")
        elif remote_url:
            split = remote_url.split("/")
            inferred_dir = os.path.join(os.getcwd() if not new_dir else new_dir, split[-1])
            if os.path.exists(inferred_dir):
                raise DoltDirectoryException(
                    f"Path already exists: {inferred_dir}. Cannot create new directory"
                )
            return inferred_dir
        elif new_dir:
            return new_dir
        else:
            raise

    @staticmethod
    def read_tables(
        remote_url: str,
        committish: str,
        tables: Optional[Union[str, List[str]]] = None,
        new_dir: Optional[str] = None,
    ) -> "Dolt":
        """
        Reads the specified tables, or all the tables, from the DoltHub database specified into a new local database,
        at the commit or branch provided. Users can optionally provide an existing directory.
        :param remote_url:
        :param committish:
        :param tables:
        :param new_dir:
        :return:
        """
        args = ["read-tables"]

        clone_dir = Dolt._get_clone_dir(new_dir, None if new_dir else remote_url)
        if not clone_dir:
            raise ValueError("Unable to infer new_dir")

        args.extend(["--dir", clone_dir, remote_url, committish])

        if tables:
            args.extend(to_list(tables))

        _execute(args, cwd=new_dir)

        return Dolt(clone_dir)

    def creds_new(self) -> bool:
        """
        Create a new set of credentials for this Dolt repository.
        :return:
        """
        args = ["creds", "new"]

        output = self.execute(args, print_output=False)

        if len(output) == 2:
            for out in output:
                logger.info(out)
        else:
            output_str = "\n".join(output)
            raise ValueError(f"Unexpected output: \n{output_str}")

        return True

    def creds_rm(self, public_key: str) -> bool:
        """
        Remove the key pair identified by the specified public key ID.
        :param public_key:
        :return:
        """
        args = ["creds", "rm", public_key]

        output = self.execute(args, print_output=False)

        if output[0].startswith("failed"):
            logger.error(output[0])
            raise DoltException("Tried to remove non-existent creds")

        return True

    def creds_ls(self) -> List[KeyPair]:
        """
        Parse the set of keys this repo has into `KeyPair` objects.
        :return:
        """
        args = ["creds", "ls", "--verbose"]

        output = self.execute(args, print_output=False)

        creds = []
        for line in output:
            if line.startswith("*"):
                active = True
                split = line[1:].lstrip().split(" ")
            else:
                active = False
                split = line.lstrip().split(" ")

            creds.append(KeyPair(split[0], split[1], active))

        return creds

    def creds_check(self, endpoint: Optional[str] = None, creds: Optional[str] = None) -> bool:
        """
        Check that credentials authenticate with the specified endpoint, return True if authorized, False otherwise.
        :param endpoint: the endpoint to check
        :param creds: creds identified by public key ID
        :return:
        """
        args = ["dolt", "creds", "check"]

        if endpoint:
            args.extend(["--endpoint", endpoint])
        if creds:
            args.extend(["--creds", creds])

        output = _execute(args, self.repo_dir)

        if output[3].startswith("error"):
            logger.error("\n".join(output[3:]))
            return False

        return True

    def creds_use(self, public_key_id: str) -> bool:
        """
        Use the credentials specified by the provided public keys ID.
        :param public_key_id:
        :return:
        """
        args = ["creds", "use", public_key_id]

        output = _execute(args, self.repo_dir)

        if output and output[0].startswith("error"):
            logger.error("\n".join(output[3:]))
            raise DoltException("Bad public key")

        return True

    def creds_import(self, jwk_filename: str, no_profile: str):
        """
        Not currently supported.
        :param jwk_filename:
        :param no_profile:
        :return:
        """
        raise NotImplementedError()

    @classmethod
    def config_global(
        cls,
        name: Optional[str] = None,
        value: Optional[str] = None,
        add: bool = False,
        list: bool = False,
        get: bool = False,
        unset: bool = False,
    ) -> Dict[str, str]:
        """
        Class method for manipulating global configs.
        :param name:
        :param value:
        :param add:
        :param list:
        :param get:
        :param unset:
        :return:
        """
        return cls._config_helper(
            global_config=True,
            cwd=os.getcwd(),
            name=name,
            value=value,
            add=add,
            list=list,
            get=get,
            unset=unset,
        )

    def config_local(
        self,
        name: Optional[str] = None,
        value: Optional[str] = None,
        add: bool = False,
        list: bool = False,
        get: bool = False,
        unset: bool = False,
    ) -> Dict[str, str]:
        """
        Instance method for manipulating configs local to a repository.
        :param name:
        :param value:
        :param add:
        :param list:
        :param get:
        :param unset:
        :return:
        """
        return self._config_helper(
            local_config=True,
            cwd=self.repo_dir,
            name=name,
            value=value,
            add=add,
            list=list,
            get=get,
            unset=unset,
        )

    @classmethod
    def _config_helper(
        cls,
        global_config: bool = False,
        local_config: bool = False,
        cwd: Optional[str] = None,
        name: Optional[str] = None,
        value: Optional[str] = None,
        add: bool = False,
        list: bool = False,
        get: bool = False,
        unset: bool = False,
    ) -> Dict[str, str]:

        switch_count = [el for el in [add, list, get, unset] if el]
        if len(switch_count) != 1:
            raise ValueError("Exactly one of add, list, get, unset must be True")

        args = ["config"]

        if global_config:
            args.append("--global")
        elif local_config:
            args.append("--local")
        else:
            raise ValueError("Must pass either global_config")

        if add:
            if not (name and value):
                raise ValueError("For add, name and value must be set")
            args.extend(["--add", name, value])
        if list:
            if name or value:
                raise ValueError("For list, no name and value provided")
            args.append("--list")
        if get:
            if not name or value:
                raise ValueError("For get, only name is provided")
            args.extend(["--get", name])
        if unset:
            if not name or value:
                raise ValueError("For get, only name is provided")
            args.extend(["--unset", name])

        output = _execute(args, cwd).split("\n")
        result = {}
        for line in [x for x in output if x is not None and "=" in x]:
            split = line.split(" = ")
            config_name, config_val = split[0], split[1]
            result[config_name] = config_val

        return result

    def ls(self, system: bool = False, all: bool = False, **kwargs) -> List[TableT]:
        """
        List the tables in the working set, the system tables, or all. Parses the tables and their object hash into an
        object that also provides row count.
        :param system:
        :param all:
        :return:
        """
        args = ["ls", "--verbose"]

        if all:
            args.append("--all")

        if system:
            args.append("--system")

        output = self.execute(args, print_output=False, **kwargs).split("\n")
        tables: List[TableT] = []
        system_pos = None

        if len(output) == 3 and output[0] == "No tables in working set":
            return tables

        for i, line in enumerate(output):
            if line.startswith("Tables") or not line:
                pass
            elif line.startswith("System"):
                system_pos = i
                break
            else:
                if not line:
                    pass
                split = line.lstrip().split()
                tables.append(Table(name=split[0], root=split[1], row_cnt=int(split[2])))

        if system_pos:
            for line in output[system_pos:]:
                if line.startswith("System"):
                    pass
                else:
                    tables.append(Table(name=line.strip(), system=True))

        return tables

    def schema_export(self, table: str, filename: Optional[str] = None):
        """
        Export the scehma of the table specified to the file path specified.
        :param table:
        :param filename:
        :return:
        """
        args = ["schema", "export", table]

        if filename:
            args.extend(["--filename", filename])
            _execute(args, self.repo_dir)
            return True
        else:
            output = _execute(args, self.repo_dir)
            logger.info("\n".join(output))
            return True

    def schema_import(
        self,
        table: str,
        filename: str,
        create: bool = False,
        update: bool = False,
        replace: bool = False,
        dry_run: bool = False,
        keep_types: bool = False,
        file_type: Optional[str] = None,
        pks: List[str] = None,
        map: Optional[str] = None,
        float_threshold: float = None,
        delim: Optional[str] = None,
    ):
        """
        This implements schema import from Dolt, it works by inferring a schema from the file provided. It operates in
        three modes: create, update, and replace. All require a table name. Create and replace require a primary key, as
        they replace an existing table with a new one with a newly inferred schema.

        :param table: name of the table to create or update
        :param filename: file to infer schema from
        :param create: create a table
        :param update: update a table
        :param replace: replace a table
        :param dry_run: output the SQL to run, do not execute it
        :param keep_types: when a column already exists, use its current type
        :param file_type: type of file used for schema inference
        :param pks: the list of primary keys
        :param map: mapping file mapping column name to new value
        :param float_threshold: minimum value fractional component must have to be float
        :param delim: the delimeter used in the file being inferred from
        :return:
        """
        switch_count = [el for el in [create, update, replace] if el]
        if len(switch_count) != 1:
            raise ValueError("Exactly one of create, update, replace must be True")

        args = ["schema", "import"]

        if create:
            args.append("--create")
            if not pks:
                raise ValueError("When create is set to True, pks must be provided")
        if update:
            args.append("--update")
        if replace:
            args.append("--replace")
            if not pks:
                raise ValueError("When replace is set to True, pks must be provided")
        if dry_run:
            args.append("--dry-run")
        if keep_types:
            args.append("--keep-types")
        if file_type:
            args.extend(["--file_type", file_type])
        if pks:
            args.extend(["--pks", ",".join(pks)])
        if map:
            args.extend(["--map", map])
        if float_threshold:
            args.extend(["--float-threshold", str(float_threshold)])
        if delim:
            args.extend(["--delim", delim])

        args.extend([str(table), str(filename)])

        self.execute(args)

    def schema_show(self, tables: Union[str, List[str]], commit: Optional[str] = None):
        """
        Dislay the schema of the specified table or tables at the (optionally) specified commit, defaulting to the tip
        of master on the current branch.
        :param tables:
        :param commit:
        :return:
        """
        args = ["schema", "show"]

        if commit:
            args.append(commit)

        args.extend(to_list(tables))

        self.execute(args)

    def table_rm(self, tables: Union[str, List[str]]):
        """
        Remove the table or list of tables provided from the working set.
        :param tables:
        :return:
        """
        self.execute(["rm", " ".join(to_list(tables))])

    def table_import(
        self,
        table: str,
        filename: str,
        create_table: bool = False,
        update_table: bool = False,
        force: bool = False,
        mapping_file: Optional[str] = None,
        pk: List[str] = None,
        replace_table: bool = False,
        file_type: Optional[str] = None,
        continue_importing: bool = False,
        delim: str = None,
    ):
        """
        Import a table from a filename, inferring the schema from the file. Operates in two possible modes, update,
        create, or replace. If creating must provide a primary key.
        :param table: the table to be created or updated
        :param filename: the data file to import
        :param create_table: create a table
        :param update_table: update a table
        :param force: force the import to overwrite existing data
        :param mapping_file: file mapping column names in file to new names
        :param pk: columns from which to build a primary key
        :param replace_table: replace existing tables
        :param file_type: the type of the file being imported
        :param continue_importing:
        :param delim:
        :return:
        """
        switch_count = [el for el in [create_table, update_table, replace_table] if el]
        if len(switch_count) != 1:
            raise ValueError("Exactly one of create, update, replace must be True")

        args = ["table", "import"]

        if create_table:
            args.append("--create-table")
            if not pk:
                raise ValueError("When create is set to True, pks must be provided")
        if update_table:
            args.append("--update-table")
        if replace_table:
            args.append("--replace-table")
            if not pk:
                raise ValueError("When replace is set to True, pks must be provided")
        if file_type:
            args.extend(["--file-type", file_type])
        if pk:
            args.extend(["--pk", ",".join(pk)])
        if mapping_file:
            args.extend(["--map", mapping_file])
        if delim:
            args.extend(["--delim", delim])
        if continue_importing:
            args.append("--continue")
        if force:
            args.append("--force")

        args.extend([table, filename])
        self.execute(args)

    def table_export(
        self,
        table: str,
        filename: str,
        force: bool = False,
        schema: Optional[str] = None,
        mapping_file: Optional[str] = None,
        pk: List[str] = None,
        file_type: Optional[str] = None,
        continue_exporting: bool = False,
    ):
        """

        :param table:
        :param filename:
        :param force:
        :param schema:
        :param mapping_file:
        :param pk:
        :param file_type:
        :param continue_exporting:
        :return:
        """
        args = ["table", "export"]

        if force:
            args.append("--force")

        if continue_exporting:
            args.append("--continue")

        if schema:
            args.extend(["--schema", schema])

        if mapping_file:
            args.extend(["--map", mapping_file])

        if pk:
            args.extend(["--pk", ",".join(pk)])

        if file_type:
            args.extend(["--file-type", file_type])

        args.extend([table, filename])
        self.execute(args)

    def table_mv(self, old_table: str, new_table: str, force: bool = False):
        """
        Rename a table from name old_table to name new_table.
        :param old_table: existing table
        :param new_table: new table name
        :param force: override changes in the working set
        :return:
        """
        args = ["table", "mv"]

        if force:
            args.append("--force")

        args.extend([old_table, new_table])
        self.execute(args)

    def table_cp(
        self,
        old_table: str,
        new_table: str,
        commit: Optional[str] = None,
        force: bool = False,
    ):
        """
        Copy an existing table to a new table, optionally at a specified commit.
        :param old_table: existing table name
        :param new_table: new table name
        :param commit: commit at which to read old_table
        :param force: override changes in the working set
        :return:
        """
        args = ["table", "cp"]

        if force:
            args.append("--force")

        if commit:
            args.append(commit)

        args.extend([old_table, new_table])
        self.execute(args)
