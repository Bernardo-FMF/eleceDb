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
