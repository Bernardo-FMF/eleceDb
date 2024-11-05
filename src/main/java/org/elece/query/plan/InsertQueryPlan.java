package org.elece.query.plan;

import org.elece.db.DatabaseStorageManager;
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
import org.elece.query.plan.step.operation.OperationStep;
import org.elece.query.plan.step.stream.StreamStep;
import org.elece.query.plan.step.tracer.TracerStep;
import org.elece.query.plan.step.validator.ValidatorStep;
import org.elece.query.plan.step.value.ValueStep;
import org.elece.query.result.ResultInfo;
import org.elece.serializer.SerializerRegistry;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class InsertQueryPlan implements QueryPlan {
    private final TracerStep<byte[]> tracerStep;

    private final ValueStep valueStep;

    private final ValidatorStep<byte[]> validatorStep;

    private final OperationStep<byte[]> operationStep;

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
    public void execute(SchemaManager schemaManager, DatabaseStorageManager databaseStorageManager,
                        ColumnIndexManagerProvider columnIndexManagerProvider,
                        SerializerRegistry serializerRegistry) throws ParserException, SerializationException,
                                                                      SchemaException, StorageException, IOException,
                                                                      ExecutionException, InterruptedException,
                                                                      DbException, BTreeException,
                                                                      DeserializationException, TcpException {
        Optional<byte[]> values = valueStep.next();
        if (values.isEmpty()) {
            return;
        }

        validatorStep.validate(values.get());

        operationStep.execute(values.get());

        tracerStep.trace(values.get());

        ResultInfo resultInfo = tracerStep.buildResultInfo();
        streamStep.stream(resultInfo);
    }
}
