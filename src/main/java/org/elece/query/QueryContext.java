package org.elece.query;

import org.elece.query.plan.step.filter.FilterStep;
import org.elece.query.plan.step.scan.ScanStep;
import org.elece.query.result.ScanInfo;

import java.util.ArrayList;
import java.util.List;

public class QueryContext {
    private final ScanInfo scanInfo;
    private final List<ScanStep> scanSteps;
    private final List<FilterStep> filterSteps;

    public QueryContext() {
        scanInfo = new ScanInfo();
        scanSteps = new ArrayList<>();
        filterSteps = new ArrayList<>();
    }

    public void addScanStep(ScanStep scanStep) {
        scanSteps.add(scanStep);
    }

    public void addFilterStep(FilterStep filterStep) {
        filterSteps.add(filterStep);
    }

    public ScanInfo getScanInfo() {
        return scanInfo;
    }

    public List<ScanStep> getScanSteps() {
        return scanSteps;
    }

    public List<FilterStep> getFilterSteps() {
        return filterSteps;
    }
}
