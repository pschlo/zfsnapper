from __future__ import annotations
from collections.abc import Collection
from collections import deque
from dataclasses import dataclass, field

from .path import Path, EMPTY_PATH


@dataclass(frozen=False, eq=False)
class Node:
    children: dict[str, Node] = field(default_factory=dict)

    exists: bool = False
    """Whether the path corresponds to existing dataset, or is just symbolic.

    Ex.: The nodes `foo` and `foo/bar` may be excluded, even though no `foo` dataset exists.
    """

    inc: bool = False
    exc: bool = False

    # Each node is either symbolic (not exists), matched, or unmatched.
    @property
    def matched(self):
        """Dataset exists, was selected and was not excluded."""
        return self.exists and self.inc and not self.exc
    @property
    def unmatched(self):
        """Dataset exists, but was not selected or was excluded."""
        return self.exists and (not self.inc or self.exc)

    contains_inc: bool = False
    contains_exc : bool = False

    contains_matched: bool = False
    contains_unmatched: bool = False

    in_inc_recurse_region: bool = False
    in_exc_recurse_region: bool = False


@dataclass
class ResolvedPaths:
    paths: set[Path]
    recursive_roots: set[Path]
    explicit_paths: set[Path]
    deepest_common_ancestor: Path


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

    included_exact = set(included_exact)
    included_recurse = set(included_recurse)
    excluded_exact = set(excluded_exact)
    excluded_recurse = set(excluded_recurse)
    all_paths = set(all_paths)
    if EMPTY_PATH in all_paths:
        raise ValueError(f"A dataset with an empty path cannot exist")

    # Create all nodes and mark
    for p in all_paths:
        ensure_node(p).exists = True
    for p in included_exact:
        ensure_node(p).inc = True
    for p in included_recurse:
        ensure_node(p).in_inc_recurse_region = True
    for p in excluded_exact:
        ensure_node(p).exc = True
    for p in excluded_recurse:
        ensure_node(p).in_exc_recurse_region = True

    # Propagate information
    propagate_inc_exc(root)
    propagate_contains(root)

    # Find node cover
    singles, groups = find_cover(
        root,
        strict_exclude=strict_exclude,
        conservative_grouping=conservative_grouping,
        allow_root_group=allow_root_group
    )

    # Find deepest common ancestor
    _deepest_common_ancestor = deepest_common_ancestor(root)

    # Collect matched paths
    matched_paths = collect_matched_paths(root)

    # Assert that node cover is correct
    assert_cover(matched_paths, singles, groups)

    return ResolvedPaths(
        paths=matched_paths,
        explicit_paths=singles,
        recursive_roots=groups,
        deepest_common_ancestor=_deepest_common_ancestor
    )


def collect_matched_paths(root: Node) -> set[Path]:
    out: set[Path] = set()
    q: deque[tuple[Node, Path]] = deque([(root, EMPTY_PATH)])

    while q:
        node, path = q.popleft()
        # Prune entire subtree if nothing matched here or below
        if not (node.matched or node.contains_matched):
            continue
        if node.matched:
            out.add(path)
        for seg, child in node.children.items():
            q.append((child, path / seg))

    return out


def propagate_contains(root: Node):
    """Bottom-up"""
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
            par.contains_matched |= node.matched or node.contains_matched
            par.contains_unmatched |= node.unmatched or node.contains_unmatched


def propagate_inc_exc(root: Node):
    """Top-down"""
    q = deque([root])
    while q:
        node = q.popleft()
        node.inc |= node.in_inc_recurse_region
        node.exc |= node.in_exc_recurse_region

        for seg, child in node.children.items():
            child.in_inc_recurse_region |= node.in_inc_recurse_region
            child.in_exc_recurse_region |= node.in_exc_recurse_region
            q.append(child)


def assert_cover(paths: set[Path], singles: set[Path], groups: set[Path]):
    # Double-check that cover is complete
    for p in paths:
        # Either covered by single, or covered by exactly one group
        _covered_by_single = p in singles
        _covered_by_groups = sum(g.is_ancestor_of(p) for g in groups)
        assert (
            (_covered_by_single and _covered_by_groups == 0)
            or
            (not _covered_by_single and _covered_by_groups == 1)
        )


def find_cover(root: Node, strict_exclude: bool, conservative_grouping: bool, allow_root_group: bool) -> tuple[set[Path], set[Path]]:
    # Traverse trie from top to bottom
    # - find cover for nodes that are kept
    # NOTE: "include" and "exclude" must not exist and are purely symbolic, while "keep" and "unsel" must exist

    singles: set[Path] = set()
    groups: set[Path] = set()
    queue: deque[tuple[Path, Node]] = deque([(EMPTY_PATH, root)])
    while queue:
        path, node = queue.popleft()

        if not node.matched and not node.contains_matched:
            # Subtree does not contain any matches; irrelevant
            continue

        # ASSERT: Node matched directly or contains matching

        if node.unmatched or (strict_exclude and node.exc):
            # Cannot pick as recursive group since node is blocked; descend
            assert not node.matched
            for seg, child in node.children.items():
                queue.append((path / seg, child))
            continue

        # ASSERT: Node is directly kept or contains kept, and is itself not illegal

        if node.contains_unmatched or (strict_exclude and node.contains_exc):
            # Cannot pick as recursive group since node contains blocked; descend
            # If we need to keep this dataset (which itself is not blocked), it must be added as single
            if node.matched:
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
            if node.matched:
                singles.add(path)
            for seg, child in node.children.items():
                queue.append((path / seg, child))
            continue

        if path == EMPTY_PATH and not allow_root_group:
            # Cannot use empty path; must descend
            assert not node.matched
            for seg, child in node.children.items():
                queue.append((path / seg, child))
            continue

        # Take and stop descend.
        # If node does not have existing children, single vs group does not matter; we choose to treat as single.
        # NOTE: At this point, node contains no existing nodes IFF node contains no kept nodes
        #   Also: Node contains no kept nodes IMPLIES node is kept
        if not node.contains_matched:
            assert node.matched
            singles.add(path)
        else:
            groups.add(path)

    return singles, groups


def deepest_common_ancestor(root: Node) -> Path:
    prefix = Path()
    node = root

    while not node.matched:
        only_edge: tuple[str, Node] | None = None

        for seg, child in node.children.items():
            if child.matched or child.contains_matched:
                # Viable edge
                if only_edge is not None:
                    # Found a second viable branch => split
                    return prefix
                only_edge = seg, child

        if only_edge is None:
            # No viable continuation
            return prefix

        prefix /= only_edge[0]
        node = only_edge[1]

    return prefix
