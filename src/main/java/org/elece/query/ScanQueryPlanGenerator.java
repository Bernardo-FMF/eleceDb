package org.elece.query;

import org.elece.db.schema.model.Table;
import org.elece.exception.query.QueryException;
import org.elece.query.path.DefaultPathNode;
import org.elece.query.path.IndexPath;
import org.elece.query.path.IndexPathFinder;
import org.elece.query.path.NodeCollection;
import org.elece.sql.parser.expression.Expression;

import java.util.Optional;
import java.util.Queue;

public class ScanQueryPlanGenerator {
    private ScanQueryPlanGenerator() {
        // private constructor
    }

    public static NodeCollection buildNodeCollection(Table table, Expression filter) throws QueryException {
        return filter.accept(new IndexPathFinder(table));
    }

    public static Optional<DefaultPathNode> findMainPath(IndexPath indexPath) {
        Queue<DefaultPathNode> nodePaths = indexPath.buildNodePathsQueue();
        if (nodePaths.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(nodePaths.poll());
    }
}
