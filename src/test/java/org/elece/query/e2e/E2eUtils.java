package org.elece.query.e2e;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.sql.analyzer.SqlAnalyzer;
import org.elece.sql.optimizer.SqlOptimizer;
import org.elece.sql.parser.SqlParser;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.statement.Statement;
import org.elece.tcp.DependencyContainer;
import org.elece.utils.BinaryUtils;
import org.elece.utils.SerializationUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class E2eUtils {
    private static final SqlAnalyzer sqlAnalyzer = new SqlAnalyzer();
    private static final SqlOptimizer sqlOptimizer = new SqlOptimizer();

    public static Statement prepareStatement(SchemaManager schemaManager, String statement) throws ParserException,
                                                                                                   TokenizerException,
                                                                                                   AnalyzerException {
        SqlParser sqlParser = new SqlParser(statement);
        Statement parsedStatement = sqlParser.parse();

        sqlAnalyzer.analyze(schemaManager, parsedStatement);
        sqlOptimizer.optimize(schemaManager, parsedStatement);

        return parsedStatement;
    }

    public static String extractValue(String attributeName, String response) {
        String regexPattern = attributeName + ":\\s*(.*)";
        Pattern pattern = Pattern.compile(regexPattern, Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(response);

        return matcher.find() ? matcher.group(1) : null;
    }

    public static void insertRow(DependencyContainer dependencyContainer, Table table, byte[] row,
                                 Object[] rowData) throws DbException, StorageException,
                                                          InterruptedTaskException,
                                                          FileChannelException,
                                                          SchemaException, BTreeException,
                                                          SerializationException {
        Pointer pointer = dependencyContainer.getDatabaseStorageManager().store(table.getId(), row);
        IndexManager<Integer, Pointer> clusterIndexManager = dependencyContainer.getColumnIndexManagerProvider().getClusterIndexManager(table);
        clusterIndexManager.addIndex((Integer) rowData[0], pointer);

        for (Column column : table.getColumns()) {
            SqlType.Type type = column.getSqlType().getType();
            Object currentColumnData = rowData[column.getId() - 1];

            if (CLUSTER_ID.equals(column.getName()) || !column.isUnique()) {
                continue;
            }

            if (type == SqlType.Type.Int) {
                IndexManager<Integer, Integer> idIndexManager = dependencyContainer.getColumnIndexManagerProvider().getIndexManager(table, column);
                idIndexManager.addIndex((Integer) currentColumnData, (Integer) rowData[0]);
            } else if (type == SqlType.Type.Varchar) {
                IndexManager<String, Integer> usernameIndexManager = dependencyContainer.getColumnIndexManagerProvider().getIndexManager(table, column);
                usernameIndexManager.addIndex((String) currentColumnData, (Integer) rowData[0]);
            }
        }
    }

    public static byte[] serializeRow(Table table, Object[] rowData) {
        byte[] row = new byte[table.getRowSize()];

        for (Column column : table.getColumns()) {
            byte[] serializedData = new byte[column.getSqlType().getSize()];
            SqlType.Type type = column.getSqlType().getType();
            Object currentColumnData = rowData[column.getId() - 1];
            if (type == SqlType.Type.Int) {
                System.arraycopy(BinaryUtils.integerToBytes((Integer) currentColumnData), 0, serializedData, 0, column.getSqlType().getSize());
            } else if (type == SqlType.Type.Bool) {
                System.arraycopy(new byte[]{(byte) (((Boolean) currentColumnData) ? 1 : 0)}, 0, serializedData, 0, column.getSqlType().getSize());
            } else {
                System.arraycopy(BinaryUtils.stringToBytes((String) currentColumnData), 0, serializedData, 0, ((String) currentColumnData).length());
                BinaryUtils.fillPadding(((String) currentColumnData).length(), serializedData.length, serializedData);
            }
            SerializationUtils.setValueOfField(table, column, serializedData, row);
        }
        return row;
    }
}
