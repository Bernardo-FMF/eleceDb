package org.elece.query.plan;

import org.elece.db.DbObject;
import org.elece.exception.*;
import org.elece.query.plan.step.deserializer.DeserializerStep;
import org.elece.query.plan.step.filter.FilterStep;
import org.elece.query.plan.step.order.OrderStep;
import org.elece.query.plan.step.scan.ScanStep;
import org.elece.query.plan.step.selector.SelectorStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;
import org.elece.query.result.ResultInfo;
import org.elece.utils.BinaryUtils;

import java.util.*;

/**
 * Represents a plan for executing a select query within the database. It is composed of multiple steps
 * including scanning, filtering, selection, ordering, deserialization, and streaming results,
 * as well as tracing the query process from start to end.
 */
public class SelectQueryPlan implements QueryPlan {
    private final Queue<ScanStep> scanSteps;
    private final Map<Long, List<FilterStep>> filterSteps;

    private final TracerStep<DbObject> startTracerStep;
    private final TracerStep<DbObject> endTracerStep;

    private final SelectorStep selectorStep;

    private final OrderStep orderStep;

    private final DeserializerStep deserializerStep;

    private final StreamStep streamStep;

    public SelectQueryPlan(Queue<ScanStep> scanSteps, Map<Long, List<FilterStep>> filterSteps,
                           TracerStep<DbObject> startTracerStep, TracerStep<DbObject> endTracerStep,
                           SelectorStep selectorStep, OrderStep orderStep, DeserializerStep deserializerStep,
                           StreamStep streamStep) {
        this.scanSteps = scanSteps;
        this.filterSteps = filterSteps;
        this.startTracerStep = startTracerStep;
        this.endTracerStep = endTracerStep;
        this.selectorStep = selectorStep;
        this.orderStep = orderStep;
        this.deserializerStep = deserializerStep;
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
     * @throws ParserException          If a parsing error occurs when filtering rows, this may happen when resolving a complex expression
     * @throws SerializationException   If a serialization error occurs
     * @throws SchemaException          If a schema-related error occurs
     * @throws StorageException         If a storage-related error occurs when executing an order by
     * @throws DbException              If a general database error occurs
     * @throws BTreeException           If a B-tree related error occurs
     * @throws DeserializationException If a deserialization error occurs when deserializing rows
     * @throws ProtoException           If a protobuf-related error occurs when streaming the results to the client
     * @throws InterruptedTaskException If the task is interrupted when executing an order by
     * @throws FileChannelException     If a file channel error occurs when executing an order by
     */
    @Override
    public void execute() throws ParserException, SerializationException, SchemaException, StorageException,
                                 DbException, BTreeException, DeserializationException, ProtoException,
                                 InterruptedTaskException, FileChannelException {
        ResultInfo startResultInfo = startTracerStep.buildResultInfo();
        streamStep.stream(startResultInfo);

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
                    endTracerStep.trace(dbObject);

                    if (!Objects.isNull(orderStep)) {
                        orderStep.addToBuffer(serializedData.get());
                    } else {
                        streamStep.stream(BinaryUtils.stringToBytes(deserializerStep.deserialize(serializedData.get())));
                    }
                }
            }
        }

        if (!Objects.isNull(orderStep)) {
            orderStep.prepareBufferState();

            Iterator<byte[]> orderedIterator = orderStep.getIterator();
            while (orderedIterator.hasNext()) {
                streamStep.stream(BinaryUtils.stringToBytes(deserializerStep.deserialize(orderedIterator.next())));
            }

            orderStep.clearBuffer();
        }

        ResultInfo resultInfo = endTracerStep.buildResultInfo();
        streamStep.stream(resultInfo);
    }
}
