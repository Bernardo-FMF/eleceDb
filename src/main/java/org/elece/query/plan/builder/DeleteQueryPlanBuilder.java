package org.elece.query.plan.builder;

import org.elece.db.DbObject;
import org.elece.query.plan.DeleteQueryPlan;
import org.elece.query.plan.step.filter.FilterStep;
import org.elece.query.plan.step.operation.OperationStep;
import org.elece.query.plan.step.scan.ScanStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;

import java.util.*;

public class DeleteQueryPlanBuilder {
    private final Queue<ScanStep> scanSteps;
    private final Map<Long, List<FilterStep>> filterSteps;

    private OperationStep<DbObject> operationStep;

    private TracerStep<DbObject> tracerStep;

    private StreamStep streamStep;

    private DeleteQueryPlanBuilder() {
        // private constructor

        this.scanSteps = new LinkedList<>();
        this.filterSteps = new HashMap<>();
    }

    public static DeleteQueryPlanBuilder builder() {
        return new DeleteQueryPlanBuilder();
    }

    public DeleteQueryPlanBuilder addScanStep(ScanStep scanStep) {
        scanSteps.add(scanStep);
        return this;
    }

    public DeleteQueryPlanBuilder addFilterStep(FilterStep filterStep) {
        List<FilterStep> newFilterSteps = this.filterSteps.computeIfAbsent(filterStep.getScanId(), _ -> new LinkedList<>());
        newFilterSteps.add(filterStep);
        return this;
    }

    public DeleteQueryPlanBuilder setOperationStep(OperationStep<DbObject> operationStep) {
        this.operationStep = operationStep;
        return this;
    }

    public DeleteQueryPlanBuilder setTracerStep(TracerStep<DbObject> tracerStep) {
        this.tracerStep = tracerStep;
        return this;
    }

    public DeleteQueryPlanBuilder setStreamStep(StreamStep streamStep) {
        this.streamStep = streamStep;
        return this;
    }

    public DeleteQueryPlan build() {
        return new DeleteQueryPlan(scanSteps, filterSteps, tracerStep, operationStep, streamStep);
    }
}
