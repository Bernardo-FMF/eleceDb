package org.elece.query.plan.builder;

import org.elece.query.plan.InsertQueryPlan;
import org.elece.query.plan.step.operation.OperationStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;
import org.elece.query.plan.step.validator.ValidatorStep;
import org.elece.query.plan.step.value.ValueStep;

public class InsertQueryPlanBuilder {
    private ValueStep valueStep;

    private ValidatorStep<byte[]> validatorStep;

    private OperationStep<byte[]> operationStep;

    private TracerStep<byte[]> tracerStep;

    private StreamStep streamStep;

    public static InsertQueryPlanBuilder builder() {
        return new InsertQueryPlanBuilder();
    }

    public InsertQueryPlanBuilder setValueStep(ValueStep valueStep) {
        this.valueStep = valueStep;
        return this;
    }

    public InsertQueryPlanBuilder setValidatorStep(ValidatorStep<byte[]> validatorStep) {
        this.validatorStep = validatorStep;
        return this;
    }

    public InsertQueryPlanBuilder setOperationStep(OperationStep<byte[]> operationStep) {
        this.operationStep = operationStep;
        return this;
    }

    public InsertQueryPlanBuilder setTracerStep(TracerStep<byte[]> tracerStep) {
        this.tracerStep = tracerStep;
        return this;
    }

    public InsertQueryPlanBuilder setStreamStep(StreamStep streamStep) {
        this.streamStep = streamStep;
        return this;
    }

    public InsertQueryPlan build() {
        return new InsertQueryPlan(tracerStep, valueStep, validatorStep, operationStep, streamStep);
    }
}
