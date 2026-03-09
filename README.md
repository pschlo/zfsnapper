# zfsnapper

Simple tool for ZFS snapshots. Subcommands:

* `zsr create`: Creates a new snapshot
* `zsr prune`: Destroys snapshots based on a keep policy
* `zsr push`: Pushes snapshots to a remote pool
* `zsr pull`: Pulls snapshots from a remote pool

## Installation
This package is not currently uploaded to PyPI. Install as follows:

1. Find your release of choice [here](https://github.com/pschlo/zfsnapper/releases)
2. Copy the link to `zfsnapper-x.x.x.tar.gz`
3. Run `python -m pip install {link}`

You may also prepend a [direct reference](https://peps.python.org/pep-0440/#direct-references), which might be desirable for a `requirements.txt`.


## Building
The `.tar.gz` file in a release is the [source distribution](https://packaging.python.org/en/latest/glossary/#term-Source-Distribution-or-sdist), which was created from the source code with `python3 -m build --sdist`. [Built distributions](https://packaging.python.org/en/latest/glossary/#term-Built-Distribution)
are not provided.



## Usage

Top-level arguments:

* `-d, --dataset`: The local dataset that the subcommand should act on
* `-r, --recursive`:  Also act on all descending datasets
* `-n, --dry-run`

#### create

Creates a snapshot with a random 64 bit hex name.

#### prune

Policy-based purging of zfs snapshots. Uses "restic forget" syntax.

Also see "https://github.com/restic/restic/blob/master/internal/restic/snapshot_policy.go" and "https://restic.readthedocs.io/en/latest/060_forget.html"

#### push/pull

Sends snapshots from source dataset to destination dataset. The newest common snapshot is always held on both sides so that it cannot be pruned/destroyed.
