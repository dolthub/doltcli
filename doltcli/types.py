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

    def add_merge_parent(self, parent: str) -> None:  # pragma: no cover
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
    def init(repo_dir: Optional[str] = None) -> "DoltT":  # pragma: no cover
        raise NotImplementedError()

    def execute(
        self, args: List[str], print_output: Optional[bool] = None
    ) -> str:  # pragma: no cover
        raise NotImplementedError()

    def status(self) -> "StatusT":  # pragma: no cover
        raise NotImplementedError()

    @staticmethod
    def version() -> str:  # pragma: no cover
        raise NotImplementedError()

    def add(self, tables: Union[str, List[str]]) -> "StatusT":  # pragma: no cover
        raise NotImplementedError()

    def reset(
        self,
        tables: Union[str, List[str]],
        hard: bool = False,
        soft: bool = False,
    ) -> None:  # pragma: no cover
        raise NotImplementedError()

    def commit(
        self,
        message: Optional[str] = ...,
        allow_empty: bool = False,
        date: Optional[datetime.datetime] = ...,
    ) -> None:  # pragma: no cover
        raise NotImplementedError()

    def merge(
        self,
        branch: str,
        message: Optional[str] = ...,
        squash: bool = False,
    ) -> None:  # pragma: no cover
        raise NotImplementedError()

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
    ) -> List:  # pragma: no cover
        raise NotImplementedError()

    def log(
        self, number: Optional[int] = None, commit: Optional[str] = None
    ) -> Dict:  # pragma: no cover
        raise NotImplementedError()

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
    ) -> None:  # pragma: no cover
        raise NotImplementedError()

    def blame(self, table_name: str, rev: Optional[str] = None) -> None:  # pragma: no cover
        raise NotImplementedError()

    def branch(
        self,
        branch_name: Optional[str] = None,
        start_point: Optional[str] = None,
        new_branch: Optional[str] = None,
        force: bool = False,
        delete: bool = False,
        copy: bool = False,
        move: bool = False,
    ) -> None:  # pragma: no cover
        raise NotImplementedError()

    def checkout(
        self,
        branch: Optional[str] = None,
        tables: Optional[Union[str, List[str]]] = None,
        checkout_branch: bool = False,
        start_point: Optional[str] = None,
    ) -> None:  # pragma: no cover
        raise NotImplementedError()

    def remote(
        self,
        add: bool = False,
        name: Optional[str] = None,
        url: Optional[str] = None,
        remove: bool = False,
    ) -> None:  # pragma: no cover
        raise NotImplementedError()

    def pull(self, remote: str = "origin") -> None:  # pragma: no cover
        raise NotImplementedError()

    def fetch(
        self,
        remote: str = "origin",
        refspecs: Optional[Union[str, List[str]]] = None,
        force: bool = False,
        **kwargs: Any,
    ) -> None:  # pragma: no cover
        raise NotImplementedError()

    @staticmethod
    def clone(
        remote_url: str,
        new_dir: Optional[str] = None,
        remote: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> "DoltT":  # pragma: no cover
        raise NotImplementedError()

    def ls(self, system: bool = False, all: bool = False) -> List[TableT]:  # pragma: no cover
        raise NotImplementedError()
