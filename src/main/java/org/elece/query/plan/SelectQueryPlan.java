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
