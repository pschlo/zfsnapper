

class ReplicationError(Exception):
    log_indent: int

    def __init__(self, msg: str, log_indent: int = 0) -> None:
        self.log_indent = log_indent
        super().__init__(msg)
