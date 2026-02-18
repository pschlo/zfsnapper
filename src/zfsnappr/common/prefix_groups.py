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


@dataclass
class Plan:
    kept_datasets: set[str]
    recursive_groups: set[str]
    single_datasets: set[str]


EMPTY_PATH: Path = ()


# For debugging
def is_prefix(g: Path, p: Path) -> bool:
    return len(g) <= len(p) and p[:len(g)] == g


def maximal_prefix_groups(included: Collection[str], excluded: Collection[str], all_datasets: Collection[str], recursive: bool, allow_root_group: bool = False) -> Plan:
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

    inc_paths = [as_path(p) for p in included]
    exc_paths = [as_path(p) for p in excluded]
    all_paths = [as_path(p) for p in all_datasets]

    # Create all nodes
    for p in all_paths:
        n = ensure_node(p)
        n.exists = True

    # Mark terminals
    for p in inc_paths:
        n = ensure_node(p)
        n.inc = True

    for p in exc_paths:
        n = ensure_node(p)
        n.exc = True


    # ---- Iterative top-down propagation for ancestor-blocking ----
    # Propagate inclusions and exclusions
    if recursive:
        _q = deque([root])
        while _q:
            node = _q.popleft()
            for child in node.children.values():
                child.inc |= node.inc
                child.exc |= node.exc
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
            par.contains_unsel |=  node.unsel or node.contains_unsel
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

    # Double check that cover is complete
    for p in kept_paths:
        # p in singles XOR p is covered by group
        assert (p in singles) != any(is_prefix(g, p) for g in groups)

    return Plan(
        kept_datasets=set(map(as_str, kept_paths)),
        single_datasets=set(map(as_str, singles)),
        recursive_groups=set(map(as_str, groups))
    )
