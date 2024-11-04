package org.elece.query.plan;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.SchemaManager;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.proto.TcpException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.sql.ParserException;
import org.elece.exception.storage.StorageException;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.query.plan.step.filter.FilterStep;
import org.elece.query.plan.step.order.OrderStep;
import org.elece.query.plan.step.scan.ScanStep;
import org.elece.query.plan.step.selector.SelectorStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;
import org.elece.query.result.ResultInfo;
import org.elece.serializer.SerializerRegistry;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class SelectQueryPlan implements QueryPlan {
    private final Queue<ScanStep> scanSteps;
    private final Map<Long, List<FilterStep>> filterSteps;

    private final TracerStep tracerStep;

    private final SelectorStep selectorStep;

    private final OrderStep orderStep;

    private final StreamStep streamStep;

    // TODO: implement limit and offset
    public SelectQueryPlan(Queue<ScanStep> scanSteps,
                           Map<Long, List<FilterStep>> filterSteps,
                           TracerStep tracerStep,
                           SelectorStep selectorStep,
                           OrderStep orderStep,
                           StreamStep streamStep) {
        this.scanSteps = scanSteps;
        this.filterSteps = filterSteps;
        this.tracerStep = tracerStep;
        this.selectorStep = selectorStep;
        this.orderStep = orderStep;
        this.streamStep = streamStep;
    }

    @Override
    public void execute(SchemaManager schemaManager, DatabaseStorageManager databaseStorageManager,
                        ColumnIndexManagerProvider columnIndexManagerProvider,
                        SerializerRegistry serializerRegistry) throws ParserException, SerializationException,
                                                                      SchemaException, StorageException, IOException,
                                                                      ExecutionException, InterruptedException,
                                                                      DbException, BTreeException,
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

                Optional<byte[]> serializedData = selectorStep.next(dbObject);

                if (serializedData.isPresent()) {
                    tracerStep.trace(dbObject);

                    if (!Objects.isNull(orderStep)) {
                        orderStep.addToBuffer(serializedData.get());
                    } else {
                        streamStep.stream(serializedData.get());
                    }
                }
            }
        }

        if (!Objects.isNull(orderStep)) {
            orderStep.prepareBufferState();

            Iterator<byte[]> orderedIterator = orderStep.getIterator();
            while (orderedIterator.hasNext()) {
                streamStep.stream(orderedIterator.next());
            }
        }

        ResultInfo resultInfo = tracerStep.buildResultInfo();
        streamStep.stream(resultInfo);
    }
}
