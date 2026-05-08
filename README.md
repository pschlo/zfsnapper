
# <div align="center"><img width="200" alt="zfsnapper icon 3" src="https://github.com/user-attachments/assets/b6d6e0f6-45ec-42bf-ac09-2624fee125d4" /> </br>zfsnapper</div>

`zfsnapper` is an opinionated command-line tool for managing ZFS snapshots.

It builds a workflow layer on top of the normal ZFS primitives: snapshots, user
properties, holds, `zfs send`, `zfs receive`, and `zpool` metadata. Instead of
manually choosing snapshot names, remembering replication base snapshots, and
hand-writing `zfs destroy` commands, you work with datasets, tags, retention
policies, replication targets, and peers.

The project takes inspiration from restic's ease of use: snapshot creation is
simple, retention is policy-based, and tags are optional metadata for selecting
subsets of snapshots. The implementation still stores its state in ZFS itself,
so pools can be inspected and repaired with ordinary ZFS tools when needed.

## Highlights

- Generated snapshot names, so names do not become a fragile policy language.
- Local and remote dataset selection with the same syntax.
- Optional snapshot tags for grouping and filtering.
- `restic forget`-style policy pruning.
- Incremental push replication to local or remote datasets.
- Peer metadata showing where datasets are pushed to or received from.
- ZFS holds protecting replication bases from pruning.
- Batched replication with tag propagation and repair after interrupted runs.
- Forwarding layouts such as `A -> B -> C`.

There is no separate `pull` command. A pull-style workflow is just a push from a
remote source dataset to a local destination dataset.

## Installation

`zfsnapper` is not currently published on PyPI. Installing from Git is therefore
the preferred path. The installed CLI is available as both `zfsnapper` and the
shorter alias `zsr`; this README uses `zsr`.

For normal command-line use, prefer an isolated tool installer such as `uv` or
`pipx`. This keeps zfsnapper and its Python dependencies separate from your
system Python and from unrelated virtual environments.

With `uv`:

```sh
uv tool install git+https://github.com/pschlo/zfsnapper.git
```

Or with `pipx`:

```sh
pipx install git+https://github.com/pschlo/zfsnapper.git
```

Install a specific release, branch, or commit by appending `@...`:

```sh
uv tool install git+https://github.com/pschlo/zfsnapper.git@v1.6.47
```

Plain `pip` works too, but installs into the currently selected Python
environment:

```sh
python -m pip install git+https://github.com/pschlo/zfsnapper.git
```


## Requirements

- Python 3.12 or newer.
- `zfs` and `zpool` available on every managed host.
- Permission to run the needed ZFS commands.
- SSH access for remote hosts.

Remote operation currently invokes the system `ssh` command and runs `zfs` or
`zpool` on the other host. That transport is an implementation detail; the CLI
is built around dataset specs.

## Dataset Specs

Most commands accept the same dataset selectors:

```txt
-d, --dataset DATASET                 include this dataset
-D, --recurse-dataset DATASET         include this dataset and descendants
-x, --exclude-dataset DATASET         exclude this dataset
-X, --recurse-exclude-dataset DATASET exclude this dataset and descendants
```

Dataset specs may include a connection prefix:

```txt
::pool/dataset
local::pool/dataset
host::pool/dataset
user@host::pool/dataset
user@host:port::pool/dataset
```

For a local dataset, use `::pool/dataset` or `local::pool/dataset`. The current
parser expects user and host tokens to contain only letters, digits, underscores,
or dashes, so SSH host aliases work well for longer DNS names.

Examples:

```sh
zsr list -D ::tank/home
zsr list -D nas::tank/home
zsr prune -D root@nas:2222::tank/home --keep-daily 14 --dry-run
```

## Core Workflows

### Create

Create snapshots without deciding on names:

```sh
zsr create -D ::tank/home
zsr create -d ::tank/vm1 -d ::tank/vm2 --tag before-upgrade
```

Snapshot names are generated automatically. Tags are optional labels, not the
retention policy. Use tags for things like `hot`, `cold`, `manual`, `offsite`,
or `before-upgrade`; use prune policies to decide how many hourly, daily, or
monthly buckets to keep.

### List

```sh
zsr list -D ::tank/home
zsr list -D ::tank/home --tag hot
zsr list -D ::tank/home --show-holds
zsr list -D ::tank/home --held-only
```

`list` shows snapshot names, datasets, tags, timestamps, holds, and peer
summary. If no dataset is specified, it lists all local datasets.

Tag filters can express alternatives and groups:

```sh
zsr list -D ::tank/home --tag hot,important --tag manual
```

This matches snapshots that have both `hot` and `important`, or snapshots that
have `manual`.

### Prune

Pruning applies a retention policy and destroys snapshots that are not kept.
Use `--dry-run` while developing a policy:

```sh
zsr prune -D ::tank/home --keep-daily 14 --keep-monthly 12 --dry-run
zsr prune -D ::tank/home --keep-hourly 24 --keep-daily 14
zsr prune -D ::tank/home --keep-within 7d
zsr prune -D ::tank/home --keep-tag important
zsr prune -D ::tank/home --keep-name 'manual-.*'
```

By default, pruning is grouped by dataset, so each dataset gets its own policy
result. zfsnapper refuses to destroy every snapshot in a group unless
`--allow-destroy-all` is passed. Snapshots with ZFS holds are reported and
skipped.

### Push

Push selected source datasets into a destination root:

```sh
zsr push -D ::tank/home backup::backup/home --init
zsr push -D ::tank/home backup::backup/home --tag offsite
zsr push -D ::tank/home backup::backup/home --batch-size 16
```

`--init` allows zfsnapper to create missing destination datasets by sending the
oldest selected source snapshot first. After that, replication is incremental.

The destination uses the same connection syntax as source selectors:

```sh
zsr push -D ::tank/home user@backup-host::backup/home --init
```

To pull from a remote host, make the remote dataset the source and the local
dataset the destination:

```sh
zsr push -D nas::tank/home ::backup/home --init
```

Encrypted source datasets use raw sends by default with `--encryption keep`.
Use `--encryption clear` for a plain send.

### Peers

Every push records peer metadata on both sides. This makes replication
relationships visible and gives pruning enough information to remove stale
peer-specific holds.

```sh
zsr peer list
zsr peer list -D ::tank/home
zsr peer prune -D ::tank/home --unused-for 90d --dry-run
zsr peer prune -D ::tank/home --sync backup --dry-run
```

`peer prune` can remove peers that are unused, unheld, unknown, no longer present
on a synced host or pool, or explicitly named.

### Tags And Holds

Existing snapshots can be tagged from their names or from another ZFS property:

```sh
zsr tag -D ::tank/home --add-from-name
zsr tag -D ::tank/home --set-from-prop com.example:backup-tags
```

Manual hold cleanup is available as an escape hatch:

```sh
zsr unhold -D ::tank/home SNAPSHOT_NAME --dry-run
```

Normally, `push`, `prune`, and `peer prune` maintain zfsnapper holds for you.

## Replication Design

zfsnapper identifies common snapshots by ZFS snapshot GUID, not by name. For
each replication relationship, it keeps a peer-specific hold on the current
incremental base on both sides. Those holds prevent pruning from removing the
snapshot needed for the next incremental send.

Replication is batched for efficiency. For consecutive snapshots, a batch can be
sent as one stream including intermediates; otherwise zfsnapper falls back to
sending snapshots one by one.

The batch order is deliberately conservative:

1. hold source snapshots that are about to be sent;
2. send the batch;
3. write zfsnapper tags on received snapshots;
4. hold the new destination batch tip;
5. release obsolete peer holds.

If a run is interrupted after receive but before tags are written, the source
snapshots are still protected. A later `push` can repair destination snapshots
whose tags are still unset by copying tags from the matching source snapshots.

Forwarding layouts are supported:

```txt
A -> B -> C
```

On relay hosts, `--keep-relay-window N` keeps snapshots near send-peer holds so
tags can still be propagated or repaired after interrupted multi-hop
replication. In practice, set it to at least the batch size used by the relevant
push jobs.

## Implementation Notes

These details are useful for debugging or manual inspection, but are not usually
needed for daily use:

- tags are stored in `zfsnapper:tags`;
- peer metadata is stored in `zfsnapper:peer:<slot>` dataset properties;
- peer holds are named `zfsnapper-sendbase-<dataset-guid>` and
  `zfsnapper-recvbase-<dataset-guid>`;
- destination datasets created by `push --init` are received with conservative
  properties such as `readonly=on`, `atime=off`, `canmount=off`, and
  `mountpoint=none`.

For exact command syntax, use:

```sh
zsr COMMAND --help
```

## Development

The project uses `uv_build` and has a `uv.lock`.

Run from a checkout:

```sh
uv run zsr --help
```

Build a source distribution:

```sh
python -m build --sdist
```
