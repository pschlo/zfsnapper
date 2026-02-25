from typing import Callable, Any
from subprocess import CalledProcessError
import logging
import threading
import time

from zfsnappr.common.zfs import ZfsCli, Snapshot, ZfsProperty, Dataset, ZfsDatasetType
from zfsnappr.common.path import Path
from zfsnappr.common.utils import space

from .exception import ReplicationError

Holdtag = str | Callable[[Dataset], str]

log = logging.getLogger(__name__)


def start_progress_thread(send_proc, on_progress: Callable[[str], Any]):
    def _reader():
        assert send_proc.stderr is not None
        for raw in iter(send_proc.stderr.readline, b""):
            line: str = raw.decode("utf-8", errors="replace").rstrip("\n")
            on_progress(line)
        send_proc.stderr.close()
    t = threading.Thread(target=_reader, daemon=True)
    t.start()
    return t


def _send_receive(
    clis: tuple[ZfsCli, ZfsCli],
    dest_dataset: Path,
    snapshot: Snapshot,
    base: Snapshot | None,
    properties: dict[str, str] = {},
    log_indent: int = 0
) -> None:
    """
    Perform a single send-receive.
    
    If base is given, it must have a hold.
    """
    def _s(level: int = 0):
        return space(log_indent + level)

    src_cli, dest_cli = clis
    send_proc, recv_proc = None, None
    terminated_send, terminated_recv = False, False

    try:
        # 1) Start sender: stdout=PIPE for data, stderr=PIPE for progress
        send_proc = src_cli.send_snapshot_async(snapshot.longname, base.longname if base else None)
        assert send_proc.stdout is not None
        assert send_proc.stderr is not None

        # 2) Start receiver, feeding it the sender's stdout
        recv_proc = dest_cli.receive_snapshot_async(dest_dataset, send_proc.stdout, properties)

        # Parent no longer needs its copy of the pipe
        send_proc.stdout.close()

        # 4) Start a thread to drain progress output
        progress_thread = start_progress_thread(send_proc, lambda s: log.info(_s() + s))

        # wait for both processes to terminate
        while True:
            send_status, recv_status = send_proc.poll(), recv_proc.poll()

            if send_status is not None and recv_status is not None:
                # both terminated
                break

            if send_status not in (None, 0) and not terminated_recv:
                # zfs send process died with error
                recv_proc.terminate()
                terminated_recv = True

            if recv_status not in (None, 0) and not terminated_send:
                # zfs receive process died with error
                send_proc.terminate()
                terminated_send = True

            time.sleep(0.1)

        progress_thread.join(timeout=1)

        # check exit codes
        for p in send_proc, recv_proc:
            if p.returncode != 0:
                raise CalledProcessError(p.returncode, cmd=p.args)
        
        # set tags on dest snapshot
        if snapshot.tags is not None:
            dest_cli.set_snapshot_tags(snapshot.with_dataset(dest_dataset).longname, snapshot.tags)
    
    except BaseException as e:
        log.info("Cleaning up")
        # On Ctrl+C or any exception, try to stop both sides.
        # terminate() is "graceful-ish"; if you need hard kill, follow with kill().
        for p in (recv_proc, send_proc):
            if p is not None and p.poll() is None:
                p.terminate()
        for p in (recv_proc, send_proc):
            if p is not None:
                try:
                    p.wait(timeout=5)
                except Exception:
                    try:
                        p.kill()
                    except Exception:
                        pass
        if isinstance(e, KeyboardInterrupt):
            raise e
        raise ReplicationError(
            f"Replication of snapshot '{snapshot.shortname}' from '{snapshot.dataset}' to '{dest_dataset}' failed",
            log_indent=log_indent
        ) from e


def send_receive_initial(
    clis: tuple[ZfsCli, ZfsCli],
    dest_dataset: Path,
    source_dataset_type: ZfsDatasetType,
    snapshot: Snapshot,
    holdtags: tuple[Callable[[Dataset], str], Callable[[Dataset], str]],
    log_indent: int = 0
) -> None:
    """Perform a single initial send-receive, thereby creating the dest dataset."""
    assert source_dataset_type in (ZfsDatasetType.FILESYSTEM, ZfsDatasetType.VOLUME)
    properties: dict[str, str] = {
        ZfsProperty.READONLY: 'on'
    }
    if source_dataset_type == ZfsDatasetType.FILESYSTEM:
        properties |= {
            ZfsProperty.ATIME: 'off',
            ZfsProperty.CANMOUNT: 'off',
            ZfsProperty.MOUNTPOINT: 'none'
        }
    _send_receive(
        clis=clis,
        dest_dataset=dest_dataset,
        snapshot=snapshot,
        base=None,
        holdtags=holdtags,
        properties=properties,
        log_indent=log_indent
    )


def send_receive_incremental(
    clis: tuple[ZfsCli, ZfsCli],
    dest_dataset: Path,
    holdtags: tuple[str,str],
    snapshot: Snapshot,
    base: Snapshot,
    log_indent: int = 0
) -> None:
    """Perform a single incremental send-receive."""
    _send_receive(
        clis=clis,
        dest_dataset=dest_dataset,
        snapshot=snapshot,
        base=base,
        holdtags=holdtags,
        log_indent=log_indent
    )
