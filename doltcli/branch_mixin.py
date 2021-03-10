from functools import wraps

def branch_defaults(f):
    @wraps(f)
    def inner(*args, **kwargs):
        init_args = ["branch"]
        if kwargs.get("force", False):
            kwargs.pop("force")
            init_args.append("--force")
        if len(args) < 2:
            f(args[0], init_args, **kwargs)
        else:
            f(args[0], init_args, *args[1:], **kwargs)
        return args[0].list_branches()
    return inner

class BranchMixin:

    @branch_defaults
    def create_branch(self, args: List[str], branch_name: Optional[str], start_point: Optional[str] = None):
        args.append(branch_name)
        if start_point is not None:
            args.append(start_point)
        return self.execute(args)

    @branch_defaults
    def delete_brawnch(self, branch_name: str):
        args.extend(["--delete", branch_name])
        return self.execute(args)

    @branch_defaults
    def move_branch(self, new_branch: str, source_branch: Optional[str] = None):
        args.append("--move")
        if source_branch is not None:
            args.append(source_branch)
        args.append(new_branch)
        return self.execute(args)

    @branch_defaults
    def copy_branch(self, args: List[str], new_branch: str, source_branch: Optional[str] = None):
        args.append("--copy")
        if branch_name is not None:
            args.append(branch_name)
        args.append(new_branch)
        return self.execute(args)

    @branch_defaults
    def list_branches(self):
        sql = f"""
            SELECT *
            FROM dolt_branches
            WHERE hash = {self.head}
        """
        dicts = read_rows_sql(self, sql=sql)
        branchs = [Branch(**d) for d in dicts]
        return branches
