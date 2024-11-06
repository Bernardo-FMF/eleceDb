package org.elece.query.result;

import org.elece.db.schema.model.Column;

import java.util.List;

public class ScanInfo {
    private final List<Column> mainScans;
    private final List<Column> secondaryFilterScans;

    public ScanInfo(List<Column> mainScans, List<Column> secondaryFilterScans) {
        this.mainScans = mainScans;
        this.secondaryFilterScans = secondaryFilterScans;
    }

    @Override
    public String toString() {
        return "ScanInfo{" +
                "mainScans=" + mainScans +
                ", secondaryFilterScans=" + secondaryFilterScans +
                '}';
    }
}
