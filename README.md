# DoltCLI

This is a minimalist package intended for data engineering applications:

- ~100kb is size unzipped
- one dependency -- Dolt
- only changes when Dolt changes

If you are a data scientist or are using Pandas there are three options:
- Use [doltpy]()
- Use [pandas.sql](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html)
  with [dolt
  sql-server](https://docs.dolthub.com/interfaces/cli#dolt-sql-server)
- Manually convert the `doltcli` return types to DataFrames with
  `pd.Dataframe.from_records(...)` or another [DataFrame instantiate of
  choice](https://pandas.pydata.org/pandas-docs/version/0.18.1/generated/pandas.DataFrame.html).

Note: `doltcli` is in beta-mode of development. The interface does not
completely wrap Dolt CLI yet, and may have function signature changes in
the short-term. Reach out to the team on our discord if you have
questions regarding production use-cases.

## Dev Setup

- clone repo
- Python 3.6+ required
- [Install Dolt binary](https://docs.dolthub.com/getting-started/installation)
- [Install Poetry](https://python-poetry.org/docs/#installation)
```bash
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
```
 -Install dependencies:
```bash
poetry install
```

Now you can run tests and use `doltcli`.
