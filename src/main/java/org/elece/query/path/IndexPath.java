package org.elece.query.path;

import java.util.*;

public class IndexPath {
    private final Set<DefaultPathNode> nodePaths;

    public IndexPath() {
        this.nodePaths = new TreeSet<>(new IndexNodeComparator());
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

    public static class IndexNodeComparator implements Comparator<DefaultPathNode> {
        @Override
        public int compare(DefaultPathNode pathNode1, DefaultPathNode pathNode2) {
            return pathNode1.getPriority() - pathNode2.getPriority();
        }
    }
}
