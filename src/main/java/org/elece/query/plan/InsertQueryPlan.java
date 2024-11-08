package org.elece.query.plan;

import org.elece.exception.*;
import org.elece.query.plan.step.operation.OperationStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;
import org.elece.query.plan.step.validator.ValidatorStep;
import org.elece.query.plan.step.value.ValueStep;
import org.elece.query.result.ResultInfo;

import java.util.Optional;

public class InsertQueryPlan implements QueryPlan {
    private final ValueStep valueStep;

    private final ValidatorStep<byte[]> validatorStep;

    private final OperationStep<byte[]> operationStep;

    private final TracerStep<byte[]> tracerStep;

    private final StreamStep streamStep;

    public InsertQueryPlan(TracerStep<byte[]> tracerStep, ValueStep valueStep, ValidatorStep<byte[]> validatorStep,
                           OperationStep<byte[]> operationStep, StreamStep streamStep) {
        this.tracerStep = tracerStep;
        this.valueStep = valueStep;
        this.validatorStep = validatorStep;
        this.operationStep = operationStep;
        this.streamStep = streamStep;
    }

    @Override
    public void execute() throws ParserException, SerializationException, SchemaException, StorageException,
                                 DbException, BTreeException, DeserializationException, ProtoException,
                                 InterruptedTaskException, FileChannelException {
        Optional<byte[]> values = valueStep.next();
        if (values.isEmpty()) {
            return;
        }

        if (validatorStep.validate(values.get())) {
            boolean executed = operationStep.execute(values.get());
            if (executed) {
                tracerStep.trace(values.get());
            }
        }

        ResultInfo resultInfo = tracerStep.buildResultInfo();
        streamStep.stream(resultInfo);
    }
}
