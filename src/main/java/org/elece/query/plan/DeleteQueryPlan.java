package org.elece.query.plan;

import org.elece.db.DbObject;
import org.elece.exception.*;
import org.elece.query.plan.step.filter.FilterStep;
import org.elece.query.plan.step.operation.OperationStep;
import org.elece.query.plan.step.scan.ScanStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;
import org.elece.query.result.ResultInfo;

import java.util.*;

/**
 * Represents a plan for executing a delete query within the database. It is composed of multiple steps
 * including scanning, filtering, the delete operation, and streaming the affected row count.
 */
public class DeleteQueryPlan implements QueryPlan {
    private final Queue<ScanStep> scanSteps;
    private final Map<Long, List<FilterStep>> filterSteps;

    private final OperationStep<DbObject> operationStep;

    private final TracerStep<DbObject> tracerStep;

    private final StreamStep streamStep;

    public DeleteQueryPlan(Queue<ScanStep> scanSteps, Map<Long, List<FilterStep>> filterSteps,
                           TracerStep<DbObject> tracerStep, OperationStep<DbObject> operationStep,
                           StreamStep streamStep) {
        this.scanSteps = scanSteps;
        this.filterSteps = filterSteps;
        this.tracerStep = tracerStep;
        this.operationStep = operationStep;
        this.streamStep = streamStep;
    }

    /**
     * Executes the delete query plan by iterating through all scan steps and
     * applying corresponding filter steps and the operation step.
     * For each one of the scan steps, which will obtain rows that may be valid to delete from disk,
     * if there are other filters associated with the current scan step, then those need to be executed as well.
     * These two steps, scan and filter, guarantee that a row matches the criteria the client defines.
     * </p>
     * The next step, is removing the row from disk, and deleting all indexed values from the respective index B-tree.
     *
     * @throws ParserException          If a parsing error occurs when filtering rows, this may happen when resolving a complex expression
     * @throws SerializationException   If a serialization error occurs
     * @throws SchemaException          If a schema-related error occurs
     * @throws StorageException         If a storage-related error occurs when deleting rows
     * @throws DbException              If a general database error occurs
     * @throws BTreeException           If a B-tree related error occurs
     * @throws DeserializationException If a deserialization error occurs when deserializing rows
     * @throws ProtoException           If a protobuf-related error occurs when streaming the results to the client
     * @throws InterruptedTaskException If the task is interrupted when executing storage or index related tasks
     * @throws FileChannelException     If a file channel error occurs when executing storage or index related tasks
     */
    @Override
    public void execute() throws ParserException, SerializationException, SchemaException, StorageException,
                                 DbException, BTreeException, DeserializationException, ProtoException,
                                 InterruptedTaskException, FileChannelException {
        while (!scanSteps.isEmpty()) {
            ScanStep rowScanner = scanSteps.poll();
            List<FilterStep> rowFilters = filterSteps.get(rowScanner.getScanId());
            if (Objects.isNull(rowFilters)) {
                rowFilters = List.of();
            }

            Optional<DbObject> scannedObject;
            while (!rowScanner.isFinished() && (scannedObject = rowScanner.next()).isPresent()) {
                DbObject dbObject = scannedObject.get();
                boolean isValid = true;

                for (FilterStep rowFilter : rowFilters) {
                    isValid = rowFilter.next(dbObject);
                    if (!isValid) {
                        break;
                    }
                }

                if (!isValid) {
                    continue;
                }

                boolean executed = operationStep.execute(dbObject);

                if (executed) {
                    tracerStep.trace(dbObject);
                }
            }
        }

        ResultInfo resultInfo = tracerStep.buildResultInfo();
        streamStep.stream(resultInfo);
    }
}
