from collections.abc import Collection

from zfsnappr.common.zfs import Dataset

def get_recv_holdtag(dataset: Dataset):
    return f"zfsnappr-recvbase-{dataset.guid}"

def get_send_holdtag(dataset: Dataset):
    return f"zfsnappr-sendbase-{dataset.guid}"

def parse_recv_holdtag(tag: str):
    return int(tag.removeprefix('zfsnappr-recvbase-'))

def parse_send_holdtag(tag: str):
    return int(tag.removeprefix('zfsnappr-sendbase-'))

def parse_send_holdtags(tags: Collection[str]):
    return {parse_send_holdtag(t) for t in tags if t.startswith('zfsnappr-sendbase-')}

def parse_recv_holdtags(tags: Collection[str]):
    return {parse_recv_holdtag(t) for t in tags if t.startswith('zfsnappr-recvbase-')}
