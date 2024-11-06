package org.elece.query.plan.builder;

import org.elece.db.DbObject;
import org.elece.query.plan.SelectQueryPlan;
import org.elece.query.plan.step.filter.FilterStep;
import org.elece.query.plan.step.order.OrderStep;
import org.elece.query.plan.step.scan.ScanStep;
import org.elece.query.plan.step.selector.SelectorStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;

import java.util.*;

public class SelectQueryPlanBuilder {
    private final Queue<ScanStep> scanSteps;
    private final Map<Long, List<FilterStep>> filterSteps;
    private TracerStep<DbObject> tracerStep;
    private SelectorStep selectorStep;
    private OrderStep orderStep;
    private StreamStep streamStep;

    private SelectQueryPlanBuilder() {
        // private constructor

        this.scanSteps = new LinkedList<>();
        this.filterSteps = new HashMap<>();
    }

    public static SelectQueryPlanBuilder builder() {
        return new SelectQueryPlanBuilder();
    }

    public SelectQueryPlanBuilder addScanStep(ScanStep scanStep) {
        scanSteps.add(scanStep);
        return this;
    }

    public SelectQueryPlanBuilder addFilterStep(FilterStep filterStep) {
        List<FilterStep> newFilterSteps = this.filterSteps.computeIfAbsent(filterStep.getScanId(), _ -> new LinkedList<>());
        newFilterSteps.add(filterStep);
        return this;
    }

    public SelectQueryPlanBuilder setTracerStep(TracerStep<DbObject> tracerStep) {
        this.tracerStep = tracerStep;
        return this;
    }

    public SelectQueryPlanBuilder setSelectorStep(SelectorStep selectorStep) {
        this.selectorStep = selectorStep;
        return this;
    }

    public SelectQueryPlanBuilder setOrderStep(OrderStep orderStep) {
        this.orderStep = orderStep;
        return this;
    }

    public SelectQueryPlanBuilder setStreamStep(StreamStep streamStep) {
        this.streamStep = streamStep;
        return this;
    }

    public SelectQueryPlan build() {
        return new SelectQueryPlan(scanSteps, filterSteps, tracerStep, selectorStep, orderStep, streamStep);
    }
}
