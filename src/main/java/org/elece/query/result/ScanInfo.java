package org.elece.query.result;

import org.elece.db.schema.model.Column;

import java.util.List;

public class ScanInfo {
    private final List<Column> indexedScanColumns;
    private final List<Column> diskScanColumns;

    public ScanInfo(List<Column> indexedScanColumns, List<Column> diskScanColumns) {
        this.indexedScanColumns = indexedScanColumns;
        this.diskScanColumns = diskScanColumns;
    }
}
