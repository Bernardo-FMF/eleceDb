package org.elece.query.plan;

import org.elece.exception.*;
import org.elece.query.plan.step.operation.OperationStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;
import org.elece.query.plan.step.validator.ValidatorStep;
import org.elece.query.plan.step.value.ValueStep;
import org.elece.query.result.ResultInfo;

import java.util.Optional;

/**
 * Represents a plan for executing an insert query within the database. It is composed of multiple steps
 * including row deserializing, value sanitizing and validation to guarantee that indexed values are unique, and stream the affected row count to the client.
 */
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

    /**
     * Executes the select query plan by iterating through all scan steps and
     * applying corresponding filter steps, selector step, and order step.
     * For each one of the scan steps, which will obtain rows that may be valid to send to the client one by one,
     * if there are other filters associated with the current scan step, then those need to be executed as well.
     * These two steps, scan and filter, guarantee that a row matches the criteria the client defines.
     * </p>
     * The next step, is manipulating the entire row to select only the specific columns contained within the statement.
     * </p>
     * If there is an order by expression, then the rows can't be streamed to the client when they are processed, instead they are first
     * stored in an in-memory buffer, and when the threshold is reached, they are flushed onto a temporary file.
     * Every flush creates a new temporary file, where all the rows are ordered using the defined column and type or ordering,
     * this will create K ordered files, so when all rows are processed, we need to read all the rows from the temporary files,
     * and since the rows are all ordered within their respective file, we can employ a variation of the K-way merge algorithm
     * to stream the results to the client.
     * If there's no order by expression, we can simply stream the rows to the client when they are processed.
     *
     * @throws ParserException          If a parsing error occurs when deserializing the new row
     * @throws SerializationException   If a serialization error occurs when processing the new row or when storing the row in disk
     * @throws SchemaException          If a schema-related error occurs
     * @throws StorageException         If a storage-related error occurs when storing the row to disk or when manipulating B-trees
     * @throws DbException              If a general database error occurs
     * @throws BTreeException           If a B-tree related error occurs
     * @throws DeserializationException If a deserialization error occurs when deserializing rows
     * @throws ProtoException           If a protobuf-related error occurs when streaming the results to the client
     * @throws InterruptedTaskException If the task is interrupted when storing to disk
     * @throws FileChannelException     If a file channel error occurs when storing to disk
     */
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
