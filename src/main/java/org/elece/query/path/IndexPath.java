package org.elece.query.path;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

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
        PriorityQueue<DefaultPathNode> newQueue = new PriorityQueue<>((nodeA, nodeB) -> nodeB.getPriority() - nodeA.getPriority());
        newQueue.addAll(nodePaths);
        return newQueue;
    }
}
