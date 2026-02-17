from collections.abc import Collection, Sequence
from collections import deque
from typing import cast, Any
from dataclasses import dataclass, field
import networkx as nx
from itertools import chain


type Path = tuple[str, ...]


def forest_bfs[T](graph: nx.DiGraph[T], *, reverse: bool = False):
    if reverse:
        graph = graph.reverse(copy=False)

    sources = [n for n, d in graph.in_degree() if d == 0]
    queue: deque[T] = deque(sources)
    while queue:
        u = queue.popleft()
        for v in graph.successors(u):
            queue.append(v)
            yield (u, v)


def as_path(path: str) -> Path:
    return tuple(x for x in path.split("/") if x)

def all_prefixes(path: Path) -> list[Path]:
    """Includes the empty path"""
    return [path[:i] for i in range(len(path)+1)]

def parent(path: Path) -> Path | None:
    """
    - Path of length 1 has parent `[]`
    - Path `[]` has parent `None`
    """
    return path[:-1] if path else None


@dataclass(frozen=False, eq=False)
class Node:
    children: dict[str, "Node"] = field(default_factory=dict)
    path: Path
    directly_inc: bool = False
    directly_exc: bool = False
    contains_inc: bool = False
    contains_exc : bool = False
    is_exc_by_ancestor: bool = False


EMPTY_PATH: Path = ()


def maximal_prefix_groups(included: Collection[str], excluded: Collection[str], exclude_blocks_descendants: bool) -> set[str]:
    trie: nx.DiGraph[Node] = nx.DiGraph()
    inc_paths: set[Path] = {as_path(p) for p in included}
    exc_paths: set[Path] = {as_path(p) for p in excluded}

    prefix_to_node: dict[Path, Node] = {}
    prefixes = {prefix for path in (inc_paths | exc_paths) for prefix in all_prefixes(path)}

    # Create root node
    root_node = Node(EMPTY_PATH)
    prefix_to_node[EMPTY_PATH] = root_node
    trie.add_node(root_node)

    # Create all prefix nodes, from shortest to longest
    for prefix in sorted(prefixes - {EMPTY_PATH}, key=len):
        # Create node
        node = Node(prefix)
        prefix_to_node[prefix] = node
        trie.add_node(node)

        # Add edge from parent to child
        _parent = parent(prefix)
        assert _parent is not None
        trie.add_edge(
            prefix_to_node[_parent],
            node,
            segment=prefix[-1]
        )

    # Assign has_inc and has_exc to prefixes
    for p in inc_paths:
        prefix_to_node[p].directly_inc = True
    for p in exc_paths:
        prefix_to_node[p].directly_exc = True

    # Traverse trie from bottom to top and determine for each node whether it contains includes and/or excludes
    for child, par in forest_bfs(trie, reverse=True):
        # Update parent with information from child
        if child.directly_inc or child.contains_inc:
            par.contains_inc = True
        if child.directly_exc or child.contains_exc:
            par.contains_exc = True

    # Traverse trie from top to bottom and check whether blocked by ancestor
    for par, child in forest_bfs(trie):
        if par.is_exc_by_ancestor or par.path in exc_paths:
            child.is_exc_by_ancestor = True

    # Traverse trie from top to bottom
    groups: set[Path] = set()
    queue: deque[Node] = deque([root_node])
    while queue:
        node = queue.popleft()
        if not (node.directly_inc or node.contains_inc):
            # Irrelevant path
            continue

        if node.directly_exc or node.contains_exc:
            # Prefix is too coarse; descend
            queue += trie.successors(node)
            continue

        if node.is_exc_by_ancestor:
            # Prefix is too coarse; dead-end since everything below is also exc_by_ancestor
            continue

        # assert: node.directly_inc or node.contains_inc
        # assert: not node.directly_exc and not node.contains_exc
        # assert: not node.is_exc_by_ancestor
        # Prefix is safe; stop descend
        groups.add(node.path)

    return {'/'.join(p) for p in groups}
