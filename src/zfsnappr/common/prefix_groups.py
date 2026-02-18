from collections.abc import Collection
from collections import deque
from typing import cast, Any
from dataclasses import dataclass, field


type Path = tuple[str, ...]


def as_path(path: str) -> Path:
    return tuple(x for x in path.split("/") if x)

def as_str(path: Path) -> str:
    return '/'.join(path)


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
class Plan:
    kept_datasets: set[str]
    recursive_groups: set[str]
    single_datasets: set[str]


EMPTY_PATH: Path = ()


def is_prefix(a: Path, b: Path) -> bool:
    """Returns whether `a` is a prefix of `b`"""
    return len(a) <= len(b) and b[:len(a)] == a

def is_under(a: Path, b: Path):
    """Whether `a` is under `b`."""
    return is_prefix(b, a)


def maximal_prefix_groups(
    all_datasets: Collection[str],
    *,
    included_exact: Collection[str] = [],
    included_recurse: Collection[str] = [],
    excluded_exact: Collection[str] = [],
    excluded_recurse: Collection[str] = [],

    allow_root_group: bool = False,
    conservative_grouping: bool = False,
) -> Plan:
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

    inc_exact = {as_path(p) for p in included_exact}
    exc_exact = {as_path(p) for p in excluded_exact}
    inc_recurse = {as_path(p) for p in included_recurse}
    exc_recurse = {as_path(p) for p in excluded_recurse}
    all_paths = {as_path(p) for p in all_datasets}
    if EMPTY_PATH in all_paths:
        raise ValueError(f"A dataset with an empty path cannot exist")

    # Create all nodes
    for p in all_paths:
        ensure_node(p).exists = True

    # Mark terminals
    for p in inc_exact:
        ensure_node(p).inc = True
    for p in inc_recurse:
        ensure_node(p).in_inc_recurse_region = True
    for p in exc_exact:
        ensure_node(p).exc = True
    for p in exc_recurse:
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

        if node.blocked or node.unsel:
            # Cannot pick as recursive group since node is blocked; descend
            assert not node.keep
            for seg, child in node.children.items():
                queue.append((path + (seg,), child))
            continue

        # ASSERT: Node is directly kept or contains kept, and is itself not illegal

        if node.contains_blocked or node.contains_unsel:
            # Cannot pick as recursive group since node contains blocked; descend
            # If we need to keep this dataset (which itself is not blocked), it must be added as single
            if node.keep:
                singles.add(path)
            for seg, child in node.children.items():
                queue.append((path + (seg,), child))
            continue

        # ASSERT: Node is directly kept or contains kept, and is itself not illegal and does not contain illegal.
        # The node is thus a suitable recursion group.

        # If the paths in all_datasets only recurse for path in inc_recursive,
        # the current group path may accidentally include unknown other trees.
        # Optionally be strict about this.
        # NOTE: node.in_inc_recurse_region IFF node path is at or under some path in inc_recursive
        if conservative_grouping and not node.in_inc_recurse_region:
            # path is not under a recursively included path; to be safe, descend
            if node.keep:
                singles.add(path)
            for seg, child in node.children.items():
                queue.append((path + (seg,), child))
            continue

        if path == EMPTY_PATH and not allow_root_group:
            # Cannot use empty path; must descend
            assert not node.keep
            for seg, child in node.children.items():
                queue.append((path + (seg,), child))
            continue

        # Take and stop descend
        groups.add(path)


    # Compute kept paths
    kept_paths = {p for p in all_paths if get_node(p).keep}

    # Double-check that cover is complete
    for p in kept_paths:
        # p in singles XOR p is covered by group
        assert (p in singles) != any(is_under(p, g) for g in groups)

    return Plan(
        kept_datasets=set(map(as_str, kept_paths)),
        single_datasets=set(map(as_str, singles)),
        recursive_groups=set(map(as_str, groups))
    )
