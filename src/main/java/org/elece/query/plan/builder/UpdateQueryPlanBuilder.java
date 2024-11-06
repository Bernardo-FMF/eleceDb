package org.elece.query.plan.builder;

import org.elece.db.DbObject;
import org.elece.query.plan.UpdateQueryPlan;
import org.elece.query.plan.step.filter.FilterStep;
import org.elece.query.plan.step.operation.OperationStep;
import org.elece.query.plan.step.scan.ScanStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;

import java.util.*;

public class UpdateQueryPlanBuilder {
    private final Queue<ScanStep> scanSteps;
    private final Map<Long, List<FilterStep>> filterSteps;

    private OperationStep<DbObject> operationStep;

    private TracerStep<DbObject> tracerStep;

    private StreamStep streamStep;

    private UpdateQueryPlanBuilder() {
        // private constructor

        this.scanSteps = new LinkedList<>();
        this.filterSteps = new HashMap<>();
    }

    public static UpdateQueryPlanBuilder builder() {
        return new UpdateQueryPlanBuilder();
    }

    public UpdateQueryPlanBuilder addScanStep(ScanStep scanStep) {
        scanSteps.add(scanStep);
        return this;
    }

    public UpdateQueryPlanBuilder addFilterStep(FilterStep filterStep) {
        List<FilterStep> newFilterSteps = this.filterSteps.computeIfAbsent(filterStep.getScanId(), _ -> new LinkedList<>());
        newFilterSteps.add(filterStep);
        return this;
    }

    public UpdateQueryPlanBuilder setOperationStep(OperationStep<DbObject> operationStep) {
        this.operationStep = operationStep;
        return this;
    }

    public UpdateQueryPlanBuilder setTracerStep(TracerStep<DbObject> tracerStep) {
        this.tracerStep = tracerStep;
        return this;
    }

    public UpdateQueryPlanBuilder setStreamStep(StreamStep streamStep) {
        this.streamStep = streamStep;
        return this;
    }

    public UpdateQueryPlan build() {
        return new UpdateQueryPlan(scanSteps, filterSteps, tracerStep, operationStep, streamStep);
    }
}
