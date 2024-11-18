package org.elece.query.path;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class IndexPath {
    private final Set<DefaultPathNode> pathNodes;
    private final Set<ComplexPathNode> complexPathNodes;

    public IndexPath() {
        this.pathNodes = new HashSet<>();
        this.complexPathNodes = new HashSet<>();
    }

    public void addPath(DefaultPathNode node) {
        pathNodes.add(node);
    }

    public void addPath(ComplexPathNode node) {
        complexPathNodes.add(node);
    }

    public Set<DefaultPathNode> getPathNodes() {
        return pathNodes;
    }

    public Set<ComplexPathNode> getComplexPathNodes() {
        return complexPathNodes;
    }

    public Queue<DefaultPathNode> buildPathNodesQueue() {
        PriorityQueue<DefaultPathNode> newQueue = new PriorityQueue<>((nodeA, nodeB) -> nodeB.getPriority() - nodeA.getPriority());
        newQueue.addAll(pathNodes);
        return newQueue;
    }
}
