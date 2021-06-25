import datetime
import json
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional, Union


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)


class BaseDataclass:
    def dict(self) -> Dict:
        return asdict(self)

    def json(self) -> str:
        return json.dumps(self.dict(), cls=Encoder)


@dataclass
class BranchT(BaseDataclass):
    name: Optional[str]
    hash: Optional[str]
    latest_committer: Optional[str] = None
    latest_committer_email: Optional[str] = None
    latest_commit_date: Optional[datetime.datetime] = None
    latest_commit_message: Optional[str] = None


@dataclass
class CommitT(BaseDataclass):
    ref: Optional[str]
    timestamp: Optional[datetime.datetime]
    author: Optional[str]
    email: Optional[str]
    message: Optional[str]
    parents: Optional[Union[List[str], str]]
    merge: bool = False

    def add_merge_parent(self, parent: str) -> None:
        ...


@dataclass
class KeyPairT(BaseDataclass):
    public_key: str
    key_id: str
    active: bool


@dataclass
class RemoteT(BaseDataclass):
    name: Optional[str]
    url: Optional[str]


@dataclass
class StatusT(BaseDataclass):
    is_clean: bool
    modified_tables: Dict[str, bool]
    added_tables: Dict[str, bool]


@dataclass
class TableT(BaseDataclass):
    name: str
    root: Optional[str] = None
    row_cnt: Optional[int] = None
    system: bool = False


@dataclass
class TagT(BaseDataclass):
    name: str
    ref: str
    message: str


@dataclass
class DoltHubContextT(BaseDataclass):
    name: Optional[str] = None
    url: Optional[str] = None


@dataclass
class DoltT:
    repo_dir: str
    print_output: bool = False

    @staticmethod
    def init(repo_dir: Optional[str] = ...) -> "DoltT":
        ...

    def execute(self, args: List[str], print_output: Optional[bool] = ...):
        ...

    def status(self) -> "StatusT":
        ...

    @staticmethod
    def version() -> str:
        ...

    def add(self, tables: Union[str, List[str]]) -> "StatusT":
        ...

    def reset(
        self,
        tables: Union[str, List[str]],
        hard: bool = False,
        soft: bool = False,
    ) -> None:
        ...

    def commit(
        self,
        message: Optional[str] = ...,
        allow_empty: bool = False,
        date: Optional[datetime.datetime] = ...,
    ) -> None:
        ...

    def merge(
        self,
        branch: str,
        message: Optional[str] = ...,
        squash: bool = False,
    ) -> None:
        ...

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
    ) -> List:
        ...

    def log(self, number: Optional[int] = ..., commit: Optional[str] = ...) -> Dict:
        ...

    def diff(
        self,
        commit: Optional[str] = ...,
        other_commit: Optional[str] = ...,
        tables: Optional[Union[str, List[str]]] = ...,
        data: bool = False,
        schema: bool = False,  # can we even support this?
        summary: bool = False,
        sql: bool = False,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> None:
        ...

    def blame(self, table_name: str, rev: Optional[str] = None) -> None:
        ...

    def branch(
        self,
        branch_name: Optional[str] = ...,
        start_point: Optional[str] = ...,
        new_branch: Optional[str] = ...,
        force: bool = False,
        delete: bool = False,
        copy: bool = False,
        move: bool = False,
    ) -> None:
        ...

    def checkout(
        self,
        branch: Optional[str] = ...,
        tables: Optional[Union[str, List[str]]] = ...,
        checkout_branch: bool = False,
        start_point: Optional[str] = ...,
    ) -> None:
        ...

    def remote(
        self,
        add: bool = False,
        name: Optional[str] = ...,
        url: Optional[str] = ...,
        remove: bool = False,
    ) -> None:
        ...

    def pull(self, remote: str = "origin") -> None:
        ...

    def fetch(
        self,
        remote: str = "origin",
        refspecs: Union[str, List[str]] = ...,
        force: bool = False,
        **kwargs: Any,
    ) -> None:
        ...

    @staticmethod
    def clone(
        remote_url: str,
        new_dir: Optional[str] = ...,
        remote: Optional[str] = ...,
        branch: Optional[str] = ...,
    ) -> "DoltT":
        ...

    def ls(self, system: bool = False, all: bool = False) -> List[TableT]:
        ...
