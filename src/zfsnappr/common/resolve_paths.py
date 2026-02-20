from __future__ import annotations
from collections.abc import Collection, Iterable
from typing import overload, SupportsIndex
from collections import deque
from dataclasses import dataclass, field


class PathError(Exception):
    pass


class Path(tuple[str, ...]):
    """A `Path` consists of 0 or more nonempty string segments.
    
    - `segment` = atomic part
    - `component` = one or more segments

    Empty segments are ignored.
    """
    def __new__(cls, *components: Path | str):
        segments = []
        for c in components:
            _segments = c if isinstance(c, Path) else c.split('/')
            segments += [s for s in _segments if s]
        return super().__new__(cls, tuple(segments))

    def __str__(self) -> str:
        return "/".join(self)

    def __repr__(self) -> str:
        return f"Path{tuple(self)!r}"

    @overload
    def __getitem__(self, key: SupportsIndex) -> str: ...
    @overload
    def __getitem__(self, key: slice) -> Path: ...
    def __getitem__(self, key: SupportsIndex | slice) -> str | Path:
        if isinstance(key, slice):
            # tuple slicing returns tuple[str, ...]; we wrap it back into Path
            return Path(*super().__getitem__(key))
        # keep tuple semantics for indexing; SupportsIndex covers things like numpy ints
        return super().__getitem__(key)
    
    def __truediv__(self, component: str) -> Path:
        return Path(*self, component)

    @property
    def depth(self) -> int:
        """Empty path has depth `0`."""
        return len(self)
    
    def covers(self, other: Path) -> bool:
        """Returns whether `self` is a prefix of `other`."""
        return len(self) <= len(other) and other[:len(self)] == self


EMPTY_PATH: Path = Path()


@dataclass(frozen=False, eq=False)
class Node:
    children: dict[str, "Node"] = field(default_factory=dict)

    exists: bool = False
    """Whether the path corresponds to existing dataset, or is just symbolic.

    Ex.: The nodes `foo` and `foo/bar` may be excluded, even though no `foo` dataset exists.
    """

    inc: bool = False
    exc: bool = False

    # Each existing dataset is either kept, blocked, or unsel.
    @property
    def keep(self):
        """Whether the dataset is finally kept or not."""
        return self.exists and self.inc and not self.exc
    @property
    def blocked(self):
        return self.exists and self.exc
    @property
    def unsel(self):
        """Used as an indicator for existing datasets that are not matched by the include/exclude policy."""
        return self.exists and not self.inc and not self.exc

    contains_inc: bool = False
    contains_exc : bool = False

    contains_keep: bool = False
    contains_blocked: bool = False
    contains_unsel: bool = False

    in_inc_recurse_region: bool = False
    in_exc_recurse_region: bool = False


@dataclass
class ResolvedPaths:
    kept_paths: set[Path]
    recursive_groups: set[Path]
    single_paths: set[Path]


def resolve_paths(
    all_paths: Collection[Path],
    *,
    included_exact: Collection[Path] = [],
    included_recurse: Collection[Path] = [],
    excluded_exact: Collection[Path] = [],
    excluded_recurse: Collection[Path] = [],

    allow_root_group: bool = False,
    conservative_grouping: bool = False,
    strict_exclude: bool = False
) -> ResolvedPaths:
    """
    Docstring for maximal_prefix_groups

    `all_datasets` must either contain all datasets, or contain at least `included_exact`, `included_recurse`, and all paths under `included_recurse`.
    
    :param included: Description
    :type included: Collection[str]
    :param excluded: Description
    :type excluded: Collection[str]
    :param all_datasets:
        Used to determine which datasets exist.
    :type all_datasets: Collection[str]
    :param allow_root_group:
        If True, the empty path is allowed as a group path.
    :type allow_root_group: bool
    :param conservative_grouping:
        If True, assume that `all_datasets` is minimal and only recurses for paths in `included_recurse`.
        To be safe and not accidentally cover an unknown dataset subtree,
        only allow grouping under paths in `included_recurse`.
    :type conservative_grouping: bool
    :param strict_exclude:
        If True, groups may not cover excluded paths even if they do not exist.
        By default, groups only avoid excluded paths that exist.
    :type strict_exclude: bool
    :return: Description
    :rtype: Plan
    """
    # Root node of the Trie; stands for the empty path and is purely symbolic,
    # i.e. root.exists == False
    root = Node()

    # Build trie from both include + exclude so the structure covers all relevant prefixes
    def ensure_node(path: Path) -> Node:
        n = root
        for seg in path:
            n = n.children.setdefault(seg, Node())
        return n

    def get_node(path: Path) -> Node:
        n = root
        for seg in path:
            n = n.children[seg]
        return n

    included_exact = set(included_exact)
    included_recurse = set(included_recurse)
    excluded_exact = set(excluded_exact)
    excluded_recurse = set(excluded_recurse)
    all_paths = set(all_paths)
    if EMPTY_PATH in all_paths:
        raise ValueError(f"A dataset with an empty path cannot exist")

    # Create all nodes
    for p in all_paths:
        ensure_node(p).exists = True

    # Mark terminals
    for p in included_exact:
        ensure_node(p).inc = True
    for p in included_recurse:
        ensure_node(p).in_inc_recurse_region = True
    for p in excluded_exact:
        ensure_node(p).exc = True
    for p in excluded_recurse:
        ensure_node(p).in_exc_recurse_region = True


    # ---- Iterative top-down propagation for ancestor-blocking ----
    # Propagate inclusions and exclusions
    _q = deque([root])
    while _q:
        node = _q.popleft()
        node.inc |= node.in_inc_recurse_region
        node.exc |= node.in_exc_recurse_region

        for seg, child in node.children.items():
            child.in_inc_recurse_region |= node.in_inc_recurse_region
            child.in_exc_recurse_region |= node.in_exc_recurse_region
            _q.append(child)


    # ---- Iterative bottom-up propagation ----
    # Gather nodes with their parents in a BFS, then process deepest-first.
    entries: list[tuple[Node, Node | None]] = []
    q: deque[tuple[Node, Node | None]] = deque([(root, None)])

    while q:
        node, par = q.popleft()
        entries.append((node, par))
        for child in node.children.values():
            q.append((child, node))

    # Deepest-first: children computed before parent
    for node, par in reversed(entries):
        if par is not None:
            par.contains_inc |= node.inc or node.contains_inc
            par.contains_exc |= node.exc or node.contains_exc
            par.contains_unsel |= node.unsel or node.contains_unsel
            par.contains_keep |= node.keep or node.contains_keep
            par.contains_blocked |= node.blocked or node.contains_blocked


    # Traverse trie from top to bottom
    # - find cover for nodes that are kept
    # - relevant are: "keep", "contains_keep"
    # (otherwise may pick group prefix that does not contain excs, but also does not cover any keeps and is thus obsolete)
    # and "contains_exc"/"contains_unsel" (to know whether tree is safe or not)
    # NOTE: "include" and "exclude" must not exist and are purely symbolic, while "keep" and "unsel" must exist

    groups: set[Path] = set()
    singles: set[Path] = set()
    queue: deque[tuple[Path, Node]] = deque([(EMPTY_PATH, root)])
    while queue:
        path, node = queue.popleft()

        if not node.keep and not node.contains_keep:
            # Subtree does not contain any keeps; irrelevant
            continue

        # ASSERT: Node is directly kept or contains kept

        if node.blocked or node.unsel or (strict_exclude and node.exc):
            # Cannot pick as recursive group since node is blocked; descend
            assert not node.keep
            for seg, child in node.children.items():
                queue.append((path / seg, child))
            continue

        # ASSERT: Node is directly kept or contains kept, and is itself not illegal

        if node.contains_blocked or node.contains_unsel or (strict_exclude and node.contains_exc):
            # Cannot pick as recursive group since node contains blocked; descend
            # If we need to keep this dataset (which itself is not blocked), it must be added as single
            if node.keep:
                singles.add(path)
            for seg, child in node.children.items():
                queue.append((path / seg, child))
            continue

        # ASSERT: Node is directly kept or contains kept, and is itself not illegal and does not contain illegal.
        # The node is thus a suitable recursion group.

        # Enforce conservative grouping.
        # NOTE: node.in_inc_recurse_region IFF node path is at or under some path in included_recurse
        if conservative_grouping and not node.in_inc_recurse_region:
            # path is not under a recursively included path; to be safe, descend
            if node.keep:
                singles.add(path)
            for seg, child in node.children.items():
                queue.append((path / seg, child))
            continue

        if path == EMPTY_PATH and not allow_root_group:
            # Cannot use empty path; must descend
            assert not node.keep
            for seg, child in node.children.items():
                queue.append((path / seg, child))
            continue

        # Take and stop descend.
        # If node does not have existing children, single vs group does not matter; we choose to treat as single.
        # NOTE: At this point, node contains no existing nodes IFF node contains no kept nodes
        #   Also: Node contains no kept nodes IMPLIES node is kept
        if not node.contains_keep:
            assert node.keep
            singles.add(path)
        else:
            groups.add(path)


    # Compute kept paths
    kept_paths = {p for p in all_paths if get_node(p).keep}

    # Double-check that cover is complete
    for p in kept_paths:
        # Either covered by single, or covered by exactly one group
        _covered_by_single = p in singles
        _covered_by_groups = sum(g.covers(p) for g in groups)
        assert (
            (_covered_by_single and _covered_by_groups == 0)
            or
            (not _covered_by_single and _covered_by_groups == 1)
        )

    return ResolvedPaths(
        kept_paths=kept_paths,
        single_paths=singles,
        recursive_groups=groups
    )
