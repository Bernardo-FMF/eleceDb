package org.elece.query.e2e;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.AnalyzerException;
import org.elece.exception.ParserException;
import org.elece.exception.TokenizerException;
import org.elece.sql.analyzer.SqlAnalyzer;
import org.elece.sql.optimizer.SqlOptimizer;
import org.elece.sql.parser.SqlParser;
import org.elece.sql.parser.statement.Statement;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
}
