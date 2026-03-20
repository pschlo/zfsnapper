

class ReplicationError(Exception):
    log_indent: int
    snaps_sent: int

    def __init__(self, msg: str, log_indent: int = 0) -> None:
        self.log_indent = log_indent
        self.snaps_sent = 0
        super().__init__(msg)
