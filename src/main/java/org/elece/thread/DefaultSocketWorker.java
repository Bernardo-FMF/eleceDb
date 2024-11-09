package org.elece.thread;

import org.elece.exception.*;
import org.elece.query.QueryPlanner;
import org.elece.query.result.ErrorResultInfo;
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
        ClientBridge clientBridge;
        InputStream inputStream;
        try {
            clientBridge = new ClientBridge(socket.getOutputStream());
            inputStream = socket.getInputStream();
        } catch (IOException exception) {
            throw new RuntimeDbException(DbError.IO_ERROR, exception.getMessage());
        }

        while (socket.isConnected()) {
            try {
                String statement = Proto.deserialize(inputStream);

                SqlParser sqlParser = new SqlParser(statement);
                Statement parsedStatement = sqlParser.parse();

                sqlAnalyzer.analyze(dependencyContainer.getSchemaManager(), parsedStatement);
                sqlOptimizer.optimize(dependencyContainer.getSchemaManager(), parsedStatement);

                QueryPlanner queryPlanner = dependencyContainer.getQueryPlanner();
                queryPlanner.plan(parsedStatement, clientBridge);
            } catch (ProtoException | ParserException | TokenizerException | SchemaException |
                     AnalyzerException | BTreeException | QueryException | SerializationException |
                     InterruptedTaskException | StorageException | DeserializationException | FileChannelException |
                     DbException exception) {
                ErrorResultInfo errorResultInfo = new ErrorResultInfo(exception.getDbError(), exception.getMessage());
                try {
                    clientBridge.send(BinaryUtils.stringToBytes(errorResultInfo.deserialize()));
                } catch (ProtoException nestedException) {
                    throw new RuntimeDbException(nestedException.getDbError(), nestedException.getMessage());
                }
            }
        }
    }
}
