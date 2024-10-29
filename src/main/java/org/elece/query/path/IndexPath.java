package org.elece.query.path;

import java.util.*;

public class IndexPath {
    private final Set<DefaultPathNode> nodePaths;

    public IndexPath() {
        // TODO: fix and use priority to order the nodes, this should be a TreeSet
        this.nodePaths = new HashSet<>();
    }

    public void addPath(DefaultPathNode node) {
        nodePaths.add(node);
    }

    public Set<DefaultPathNode> getNodePaths() {
        return nodePaths;
    }

    public Optional<DefaultPathNode> getFirst() {
        return nodePaths.stream().findFirst();
    }

    public int size() {
        return nodePaths.size();
    }

    public boolean isEmpty() {
        return nodePaths.isEmpty();
    }

    public Queue<DefaultPathNode> buildNodePathsQueue() {
        return new PriorityQueue<>(nodePaths);
    }
}
