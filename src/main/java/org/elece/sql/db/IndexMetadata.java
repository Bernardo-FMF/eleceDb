package org.elece.sql.db;

import org.elece.sql.parser.expression.internal.Column;

public record IndexMetadata(int pageNumber, String name, Column column, Schema schema, boolean unique) {
}
