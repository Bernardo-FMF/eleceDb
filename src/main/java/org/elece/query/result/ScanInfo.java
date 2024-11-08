package org.elece.query.result;

import org.elece.db.schema.model.Column;

import java.util.ArrayList;
import java.util.List;

public class ScanInfo {
    private final List<Column> mainScans;
    private final List<Column> secondaryFilterScans;

    public ScanInfo() {
        this.mainScans = new ArrayList<>();
        this.secondaryFilterScans = new ArrayList<>();
    }

    public void addMainScan(Column column) {
        this.mainScans.add(column);
    }

    public void addSecondaryFilterScan(Column column) {
        this.secondaryFilterScans.add(column);
    }

    @Override
    public String toString() {
        return "ScanInfo{" +
                "mainScans=" + mainScans +
                ", secondaryFilterScans=" + secondaryFilterScans +
                '}';
    }
}
