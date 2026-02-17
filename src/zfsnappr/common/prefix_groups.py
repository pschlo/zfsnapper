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
    directly_inc: bool = False
    directly_exc: bool = False
    contains_inc: bool = False
    contains_exc : bool = False
    exc_by_ancestor: bool = False


EMPTY_PATH: Path = ()


def maximal_prefix_groups(included: Collection[str], excluded: Collection[str], exclude_blocks_descendants: bool, allow_root_group: bool = False) -> tuple[set[str], set[str]]:
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

    # Mark terminals
    for p in inc_paths:
        ensure_node(p).directly_inc = True
    for p in exc_paths:
        ensure_node(p).directly_exc = True


    # ---- Iterative bottom-up propagation (no recursion) ----
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
            par.contains_inc = par.contains_inc or node.contains_inc or node.directly_inc
            par.contains_exc = par.contains_exc or node.contains_exc or node.directly_exc


    # ---- Iterative top-down propagation for ancestor-blocking ----
    if exclude_blocks_descendants:
        _q = deque([root])
        while _q:
            node = _q.popleft()
            for child in node.children.values():
                child.exc_by_ancestor = node.exc_by_ancestor or node.directly_exc
                _q.append(child)


    # Traverse trie from top to bottom
    groups: set[Path] = set()
    queue: deque[tuple[Path, Node]] = deque([(EMPTY_PATH, root)])
    while queue:
        path, node = queue.popleft()

        if not (node.directly_inc or node.contains_inc):
            # Irrelevant path
            continue

        if node.directly_exc or node.contains_exc:
            # Prefix is too coarse; descend
            for seg, child in node.children.items():
                queue.append((path + (seg,), child))
            continue

        if node.exc_by_ancestor:
            # Prefix is too coarse; dead-end since everything below is also exc_by_ancestor.
            # This assumes that parent.exc_by_ancestor implies child.exc_by_ancestor,
            # i.e. transitivity.
            continue

        # assert: node.directly_inc or node.contains_inc
        # assert: not node.directly_exc and not node.contains_exc
        # assert: not node.is_exc_by_ancestor
        # Prefix is safe.

        if path == EMPTY_PATH and not allow_root_group:
            # Cannot use empty path; must descend
            for seg, child in node.children.items():
                queue.append((path + (seg,), child))
            continue

        # Take and stop descend
        groups.add(path)


    # Compute kept paths
    def _keep_path(path: Path) -> bool:
        n = get_node(path)
        return not n.directly_exc and not n.exc_by_ancestor
    kept_paths = filter(_keep_path, inc_paths)

    return (
        set(map(as_str, kept_paths)),
        set(map(as_str, groups))
    )
