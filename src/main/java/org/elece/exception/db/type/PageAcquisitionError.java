package org.elece.exception.db.type;

import org.elece.exception.DbError;

public class PageAcquisitionError implements DbError {
    private final String pageName;

    public PageAcquisitionError(String pageName) {
        this.pageName = pageName;
    }

    @Override
    public String format() {
        return String.format("Failed to acquire page %s", pageName);
    }
}
