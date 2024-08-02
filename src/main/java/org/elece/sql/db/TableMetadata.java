package org.elece.sql.db;

import java.util.List;

public record TableMetadata(int pageNumber, String name, Schema schema, List<IndexMetadata> indexMetadata, Long rowId) {
}
