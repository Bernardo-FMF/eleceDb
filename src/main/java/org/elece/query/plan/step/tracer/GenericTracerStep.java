package org.elece.query.plan.step.tracer;

import org.elece.query.result.GenericQueryResultInfo;
import org.elece.query.result.ResultInfo;
import org.elece.query.result.builder.GenericQueryResultInfoBuilder;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Traces each value and increments the row counter. This row counter is useful to know how many rows were affected by the encapsulating statement.
 *
 * @param <V> The type of the input being traced.
 */
public class GenericTracerStep<V> extends TracerStep<V> {
    private final AtomicInteger rowCounter;
    private final GenericQueryResultInfo.QueryType queryType;

    public GenericTracerStep(GenericQueryResultInfo.QueryType queryType) {
        this.rowCounter = new AtomicInteger(0);
        this.queryType = queryType;
    }

    @Override
    public void trace(V value) {
        rowCounter.incrementAndGet();
    }

    @Override
    public ResultInfo buildResultInfo() {
        return GenericQueryResultInfoBuilder.builder()
                .setQueryType(queryType)
                .setAffectedRowCount(rowCounter.get())
                .build();
    }
}
