package org.elece.query.path;

import java.util.HashSet;
import java.util.Set;

public class NodeCollection {
    private final Set<IndexPath> indexPaths;

    public NodeCollection() {
        this.indexPaths = new HashSet<>();
    }

    public boolean isEmpty() {
        return this.indexPaths.isEmpty();
    }

    public void mergePath(IndexPath indexPath) {
        for (IndexPath path : this.indexPaths) {
            for (DefaultPathNode node : indexPath.getNodePaths()) {
                path.addPath(node);
            }
        }
    }

    public void addPath(IndexPath indexPath) {
        this.indexPaths.add(indexPath);
    }

    public Set<IndexPath> getIndexPaths() {
        return this.indexPaths;
    }

    public int size() {
        return this.indexPaths.size();
    }
}
