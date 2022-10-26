def sort_nodes(nodes):
    # nodes must be hashable, they might not be directly comparable
    return tuple(sorted(nodes, key=lambda x: repr(x)))