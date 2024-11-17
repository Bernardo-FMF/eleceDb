package org.elece.thread;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elece.exception.*;
import org.elece.query.QueryPlanner;
import org.elece.query.result.ErrorResultInfo;
import org.elece.query.result.builder.ErrorResultInfoBuilder;
import org.elece.sql.analyzer.SqlAnalyzer;
import org.elece.sql.optimizer.SqlOptimizer;
import org.elece.sql.parser.SqlParser;
import org.elece.sql.parser.statement.Statement;
import org.elece.tcp.DependencyContainer;
import org.elece.tcp.proto.Proto;
import org.elece.utils.BinaryUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class DefaultSocketWorker implements SocketWorker {
    private final Logger logger = LogManager.getLogger(DefaultSocketWorker.class);

    private final Socket socket;
    private final DependencyContainer dependencyContainer;
    private final SqlOptimizer sqlOptimizer;
    private final SqlAnalyzer sqlAnalyzer;

    public DefaultSocketWorker(Socket socket, DependencyContainer dependencyContainer) {
        this.socket = socket;
        this.dependencyContainer = dependencyContainer;
        this.sqlOptimizer = new SqlOptimizer();
        this.sqlAnalyzer = new SqlAnalyzer();
    }

    @Override
    public void run() {
        ClientInterface clientInterface;
        InputStream inputStream;
        try {
            clientInterface = new DefaultClientInterface(socket.getOutputStream());
            inputStream = socket.getInputStream();
        } catch (IOException exception) {
            throw new RuntimeDbException(DbError.IO_ERROR, exception.getMessage());
        }

        while (socket.isConnected()) {
            try {
                String query = Proto.deserialize(inputStream);
                logger.debug("Received query: {}", query);

                SqlParser sqlParser = new SqlParser(query);
                Statement parsedStatement = sqlParser.parse();

                sqlAnalyzer.analyze(dependencyContainer.getSchemaManager(), parsedStatement);
                sqlOptimizer.optimize(dependencyContainer.getSchemaManager(), parsedStatement);

                logger.debug("Parsed statement: {}", parsedStatement);

                QueryPlanner queryPlanner = dependencyContainer.getQueryPlanner();
                queryPlanner.plan(parsedStatement, clientInterface);
            } catch (ProtoException | ParserException | TokenizerException | SchemaException |
                     AnalyzerException | BTreeException | QueryException | SerializationException |
                     InterruptedTaskException | StorageException | DeserializationException | FileChannelException |
                     DbException exception) {
                logger.error(String.format("Error type: %s; Error message: %s", exception.getDbError(), exception.getMessage()), exception);
                ErrorResultInfo errorResultInfo = ErrorResultInfoBuilder.builder()
                        .setDbError(exception.getDbError())
                        .setMessage(exception.getMessage())
                        .build();
                try {
                    clientInterface.send(BinaryUtils.stringToBytes(errorResultInfo.deserialize()));
                } catch (ProtoException nestedException) {
                    throw new RuntimeDbException(nestedException.getDbError(), nestedException.getMessage());
                }
            }
        }
    }
}
