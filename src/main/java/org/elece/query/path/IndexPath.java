package org.elece.query.path;

import java.util.*;

public class IndexPath {
    private final Set<DefaultPathNode> nodePaths;

    public IndexPath() {
        this.nodePaths = new HashSet<>();
    }

    public void addPath(DefaultPathNode node) {
        nodePaths.add(node);
    }

    public Set<DefaultPathNode> getNodePaths() {
        return nodePaths;
    }

    public int size() {
        return nodePaths.size();
    }

    public boolean isEmpty() {
        return nodePaths.isEmpty();
    }

    public Queue<DefaultPathNode> buildNodePathsQueue() {
        PriorityQueue<DefaultPathNode> newQueue = new PriorityQueue<>(Comparator.comparingInt(DefaultPathNode::getPriority));
        newQueue.addAll(nodePaths);
        return newQueue;
    }
}
