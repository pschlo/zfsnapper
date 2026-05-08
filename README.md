# zfsnapper

`zfsnapper` is an opinionated command-line tool for managing ZFS snapshots.

The main idea is that a snapshot workflow should be described in terms of
datasets, tags, retention policies, replication targets, and peers. Snapshot
names, replication base holds, remote command execution, peer bookkeeping, and
tag repair are handled by the tool.

## What It Abstracts

ZFS already has the primitives: snapshots, user properties, holds, `zfs send`,
`zfs receive`, and `zpool` metadata. `zfsnapper` builds a workflow layer on top
of those primitives:

- snapshot names are generated automatically, so users can stop encoding policy
  in names;
- tags become the human-facing way to group and select snapshots;
- local and remote datasets use the same selector syntax;
- pruning is expressed as a policy instead of a list of `zfs destroy` calls;
- replication keeps track of safe incremental bases automatically;
- pushed/received peers are recorded as dataset metadata;
- interrupted replication can be resumed or repaired without a separate state
  database.

The implementation still deliberately uses normal ZFS state. If you inspect a
pool manually, the relevant information is stored as ZFS user properties and ZFS
holds.

The project takes inspiration from restic's user model: make the common backup
workflow easy, keep retention policy separate from snapshot creation, and use
tags as optional metadata for selecting subsets of snapshots. In that analogy,
a ZFS dataset is the managed backup scope, similar to a restic repository or
path selection, and a ZFS snapshot is the retained point-in-time object.

## Current Features

- Create snapshots across one or more datasets with generated names.
- Tag snapshots at creation time or derive tags later from names/properties.
- List snapshots with tags, holds, and peer information.
- Select datasets locally or over SSH with include/exclude rules.
- Prune snapshots with `restic forget`-style retention policies.
- Keep tagged or regex-matching snapshots regardless of age.
- Push snapshots to local or remote datasets with incremental send/receive.
- Preserve encrypted sends by default when the source dataset is encrypted.
- Transfer snapshots in batches for efficient replication.
- Propagate zfsnapper tags to the destination after receiving snapshots.
- Repair missing destination tags on the next push when an earlier run was
  interrupted.
- Track replication peers and their latest use directly on datasets.
- Prune stale peer metadata and the holds belonging to those peers.
- Support forwarding layouts such as `A -> B -> C` with relay-aware pruning.

There is no separate `pull` command. Replication is modeled as `push` from a
source selection to a destination root, and a pull-style workflow can be achieved
by running `push` locally with a remote source dataset and a local destination.

## Requirements

- Python 3.12 or newer.
- `zfs` and `zpool` available on every managed host.
- Permission to run the needed ZFS commands.
- SSH access for remote hosts.

Remote operation currently invokes the system `ssh` command and runs `zfs` or
`zpool` on the other host. That transport is an implementation detail; the main
interface is the dataset spec syntax described below.

## Installation

`zfsnapper` is not currently published on PyPI. Installing from Git is therefore
the preferred path. The installed CLI is available as both `zfsnapper` and the
shorter alias `zsr`.

With `uv`:

```sh
uv tool install git+https://github.com/pschlo/zfsnapper.git
```

Install a specific release, branch, or commit:

```sh
uv tool install git+https://github.com/pschlo/zfsnapper.git@v1.6.47
```

`pip` can also install Python packages directly from Git:

```sh
python -m pip install git+https://github.com/pschlo/zfsnapper.git
```

For a requirements file, use a direct reference:

```txt
zfsnapper @ git+https://github.com/pschlo/zfsnapper.git@v1.6.47
```

## Mental Model

### Snapshots Have Generated Names

`zsr create` generates a random short snapshot name. The name is only an
identifier. Human meaning should usually live in tags, not in the snapshot name.

```sh
zsr create -D ::tank/home
zsr create -d ::tank/vm1 -d ::tank/vm2 --tag before-upgrade
```

This avoids a common ZFS snapshot-management problem: names start as convenient
labels and eventually become a fragile policy language. zfsnapper treats names
as stable handles and tags as metadata.

### Tags Are Optional Selection Metadata

Tags are optional labels for selecting a subset of snapshots, similar to restic tags. They are useful when snapshots represent different classes of data or intent, for example `hot`, `cold`, `manual`, `offsite`, or `before-upgrade`. They are then usable by `list`, `prune`, and `push`.

```sh
zsr list -D ::tank/home --tag hot
zsr prune -D ::tank/home --tag hot --keep-daily 14
zsr push -D ::tank/home backup::backup/home --tag offsite
```

A single `--tag` value can contain a comma-separated group of tags, and multiple
`--tag` values are alternatives:

```sh
zsr list -D ::tank/home --tag hot,important --tag manual
```

This matches snapshots that have both `hot` and `important`, or snapshots that
have `manual`.

Special tag filters:

- `--tag UNSET` matches snapshots where zfsnapper's tag property is not set.
- `--tag ''` matches snapshots where the tag property is set but empty.

### Dataset Selectors Work Locally And Remotely

Most commands accept the same dataset selector options:

```sh
-d DATASET    include this dataset
-D DATASET    include this dataset and its descendants
-x DATASET    exclude this dataset
-X DATASET    exclude this dataset and its descendants
```

Dataset specs may include a connection prefix:

```txt
::pool/dataset
host::pool/dataset
user@host::pool/dataset
user@host:port::pool/dataset
local::pool/dataset
```

For a local dataset, use either `::pool/dataset` or `local::pool/dataset`. The
current parser expects user and host tokens to contain only letters, digits,
underscores, or dashes; SSH host aliases work well for remote systems with
longer DNS names.

Examples:

```sh
zsr list -D ::tank/home
zsr list -D nas::tank/home
zsr prune -D root@nas:2222::tank/home --keep-daily 14 --dry-run
```

This is one of the main abstractions in the project: once a dataset is selected,
the command generally does not care whether it is local or remote.

## Common Workflows

### Create Snapshots

Create one snapshot across a recursive dataset selection:

```sh
zsr create -D ::tank/home
```

Create the same generated snapshot name on several datasets:

```sh
zsr create -d ::tank/vm1 -d ::tank/vm2 --tag before-upgrade
```

The snapshot is created with the zfsnapper tag metadata already attached.

### List Snapshots

```sh
zsr list -D ::tank/home
zsr list -D ::tank/home --tag hot
zsr list -D ::tank/home --show-holds
zsr list -D ::tank/home --held-only
```

`list` shows the short snapshot name, dataset, tags, timestamp, holds, and peer
summary. If no dataset is specified, it lists all local datasets.

### Add Or Repair Tags

Tags can be set or added from existing snapshot names:

```sh
zsr tag -D ::tank/home --set-from-name
zsr tag -D ::tank/home --add-from-name
```

`--set-from-name` and `--add-from-name` split the snapshot name on `_`. For
example, `abc123_hot_offsite` contributes `hot` and `offsite`.

Tags can also be copied from another ZFS property:

```sh
zsr tag -D ::tank/home --set-from-prop com.example:backup-tags
zsr tag -D ::tank/home --add-from-prop com.example:backup-tags
```

Snapshot short names may be passed at the end to restrict the operation:

```sh
zsr tag -D ::tank/home --add-from-name abc123_hot_offsite
```

### Prune With A Policy

Pruning applies retention rules and then destroys the snapshots that are not
kept.

Always use `--dry-run` first when developing a policy:

```sh
zsr prune -D ::tank/home --keep-daily 14 --keep-monthly 12 --dry-run
```

Count-based retention:

```sh
zsr prune -D ::tank/home --keep-last 20
zsr prune -D ::tank/home --keep-hourly 24 --keep-daily 14
zsr prune -D ::tank/home --keep-weekly 8 --keep-yearly 5
```

Time-window retention:

```sh
zsr prune -D ::tank/home --keep-within 7d
zsr prune -D ::tank/home --keep-within-hourly 48h --keep-within-daily 90d
```

Other keep rules:

```sh
zsr prune -D ::tank/home --keep-tag important
zsr prune -D ::tank/home --keep-name 'manual-.*'
```

By default, pruning is grouped by dataset, so each dataset gets its own
retention policy. Use `--group-by ''` to apply one policy across the selected
snapshots as a whole.

Safety behavior:

- snapshots with any ZFS hold are skipped;
- zfsnapper refuses to destroy every snapshot in a group unless
  `--allow-destroy-all` is passed;
- passing explicit snapshot names also allows destroying all matched snapshots.

### Push To Another Dataset Tree

Push selected source datasets into a destination root:

```sh
zsr push -D ::tank/home backup::backup/home --init
```

The destination uses the same connection syntax:

```txt
user@host:port::pool/dataset
```

For example:

```sh
zsr push -D ::tank/home user@backup-host::backup/home --init
```

`push` maps the selected source datasets under their deepest common ancestor
into the destination root. If `tank/home/alice` and `tank/home/bob` are selected
and the destination root is `backup/home`, the destination datasets become
`backup/home/alice` and `backup/home/bob`.

Useful options:

```sh
zsr push -D ::tank/home backup::backup/home --tag offsite
zsr push -D ::tank/home backup::backup/home --exclude-tag scratch
zsr push -D ::tank/home backup::backup/home --batch-size 16
zsr push -D ::tank/home backup::backup/home --encryption keep
zsr push -D ::tank/home backup::backup/home --encryption clear
zsr push -D ::tank/home backup::backup/home --rollback
```

`--init` allows zfsnapper to create a missing destination dataset by sending the
oldest selected source snapshot. Without `--init`, missing destinations are an
error.

`--encryption keep` is the default and uses raw send when the source dataset is
encrypted. `--encryption clear` uses a plain send.

There is no special pull mode. To pull from a remote host, select the remote
dataset as the source and a local dataset as the destination:

```sh
zsr push -D nas::tank/home ::backup/home --init
```

### Inspect And Prune Peers

Every push records peer metadata on both sides. This lets zfsnapper show where a
dataset is being sent to or received from.

```sh
zsr peer list
zsr peer list -D ::tank/home
zsr peer list -D backup::backup/home
```

Peer metadata can be pruned when it is stale:

```sh
zsr peer prune -D ::tank/home --unused-for 90d --dry-run
zsr peer prune -D ::tank/home --unheld --dry-run
zsr peer prune -D ::tank/home --unknown --dry-run
```

You can also prune a specific peer:

```sh
zsr peer prune -D ::tank/home backup::backup/home --dry-run
```

Or synchronize peer metadata against the datasets that currently exist on a
host or pool:

```sh
zsr peer prune -D ::tank/home --sync backup --dry-run
zsr peer prune -D ::tank/home --sync backup::backup --dry-run
```

`peer prune` removes both the peer metadata and the zfsnapper holds belonging to
that peer.

### Release zfsnapper Holds Manually

Normally, holds are maintained by `push`, `prune`, and `peer prune`. If needed,
`unhold` releases zfsnapper-managed holds from matching snapshots:

```sh
zsr unhold -D ::tank/home SNAPSHOT_NAME --dry-run
zsr unhold -D ::tank/home SNAPSHOT_NAME
```

Only holds whose names start with `zfsnapper` are released.

## Replication Design

The replication code is designed to make incremental sends efficient while
keeping enough state around to recover from interrupted runs.

### Common Snapshot By GUID

zfsnapper uses ZFS snapshot GUIDs to find the latest common snapshot between
source and destination. Snapshot names are not trusted as identity.

Once a common snapshot is found, zfsnapper ensures that it is held on both
sides:

- the source gets a send-side hold for that destination dataset;
- the destination gets a receive-side hold for that source dataset.

Those holds prevent pruning from deleting the base needed by the next
incremental send.

### Batched Sends

`--batch-size N` lets zfsnapper transfer snapshots in batches. When the planned
snapshots are consecutive, a batch can be sent as one incremental stream that
includes intermediates. Otherwise, zfsnapper falls back to sending the snapshots
one by one.

The batch protocol is conservative:

1. hold the source snapshots that are about to be sent;
2. send the batch;
3. write zfsnapper tags onto the received destination snapshots;
4. hold the new destination batch tip;
5. release obsolete source and destination peer holds.

This order is intentional. If a run is interrupted after receive but before tags
are written, the source snapshots are still protected. A later `push` can then
repair destination snapshots whose tags are still unset by copying tags from
the matching source snapshots.

### Forwarding Chains

Forwarding layouts are explicitly supported:

```txt
A -> B -> C
```

Host `B` may need to keep snapshots both because it received them from `A` and
because it still has to forward them to `C`. The `--keep-relay-window N` prune
option keeps snapshots near send-peer holds so tags can still be propagated or
repaired after interrupted multi-hop replication.

In practice, set `--keep-relay-window` to at least the batch size used by the
push job feeding or leaving that relay host.

## Command Reference

```txt
zsr list
zsr create
zsr prune
zsr push
zsr tag
zsr unhold
zsr peer list
zsr peer prune
zsr version
```

`zfsnapper` is the long-form alias, so `zsr list` and `zfsnapper list` are
equivalent.

Most commands accept dataset selectors:

```txt
-d, --dataset DATASET
-D, --recurse-dataset DATASET
-x, --exclude-dataset DATASET
-X, --recurse-exclude-dataset DATASET
```

`-n, --dry-run` is implemented for destructive maintenance operations such as
`prune`, `peer prune`, and `unhold`.

For exact parser output, use:

```sh
zsr COMMAND --help
```

## Implementation Details

These details are useful for debugging or manually inspecting pools. They are
not usually needed for day-to-day use.

- Snapshot tags are stored in the ZFS user property `zfsnapper:tags`.
- Dataset peer slots are stored as `zfsnapper:peer:0`,
  `zfsnapper:peer:1`, and so on.
- Send-side peer holds are named `zfsnapper-sendbase-<dataset-guid>`.
- Receive-side peer holds are named `zfsnapper-recvbase-<dataset-guid>`.
- Peer metadata stores direction, dataset GUID, host, path, pool GUID, and last
  used timestamp.
- Destination datasets created by `push --init` are received with conservative
  properties. Filesystems are received with `readonly=on`, `atime=off`,
  `canmount=off`, and `mountpoint=none`.
- The current remote implementation uses the system `ssh` command with the
  remote `zfs` and `zpool` commands. The rest of the code is written around a
  `ZfsCli` abstraction, so the transport can change later.

## Development And Building

The project uses `uv_build` and has a `uv.lock`.

Run from a checkout:

```sh
uv run zsr --help
```

Build a source distribution:

```sh
python -m build --sdist
```

Built distributions are not necessary for the recommended Git-based install
flow, but standard Python packaging workflows still work if release artifacts
are useful for your environment.
