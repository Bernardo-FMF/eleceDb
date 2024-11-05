package org.elece.query.plan;

import org.elece.db.DbObject;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.proto.TcpException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.sql.ParserException;
import org.elece.exception.storage.StorageException;
import org.elece.query.plan.step.filter.FilterStep;
import org.elece.query.plan.step.operation.OperationStep;
import org.elece.query.plan.step.scan.ScanStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;
import org.elece.query.result.ResultInfo;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class UpdateQueryPlan implements QueryPlan {
    private final Queue<ScanStep> scanSteps;
    private final Map<Long, List<FilterStep>> filterSteps;

    private final OperationStep<DbObject> operationStep;

    private final TracerStep<DbObject> tracerStep;

    private final StreamStep streamStep;

    public UpdateQueryPlan(Queue<ScanStep> scanSteps, Map<Long, List<FilterStep>> filterSteps,
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
                                 IOException, ExecutionException, InterruptedException, DbException, BTreeException,
                                 DeserializationException, TcpException {
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
