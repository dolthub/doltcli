import datetime

from doltcli import Branch

dt = datetime.datetime.strptime("2018-06-29", "%Y-%m-%d")


def test_datetime_serialize():
    cmp = dict(
        name="test",
        hash="23",
        latest_committer=None,
        latest_commit_date=dt,
        latest_committer_email=None,
        latest_commit_message=None,
        remote=None,
        branch=None,
    )
    br = Branch(**cmp)
    assert br.dict() == cmp
    assert (
        br.json()
        == """
            {"name": "test", "hash": "23", "latest_committer": null, "latest_committer_email": null, "latest_commit_date": "2018-06-29 00:00:00", "latest_commit_message": null, "remote": null, "branch": null}
            """.strip()
    )
