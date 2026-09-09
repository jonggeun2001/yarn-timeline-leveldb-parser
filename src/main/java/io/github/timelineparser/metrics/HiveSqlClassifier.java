package io.github.timelineparser.metrics;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.HiveLexer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

/** Classifies the outer Hive statement, without executing Hive or resolving tables. */
final class HiveSqlClassifier {
    enum Kind { SELECT, CTAS, INSERT_INTO, INSERT_OVERWRITE, INSERT_DIRECTORY, UNSUPPORTED }

    private HiveSqlClassifier() { }

    static Kind classify(String sql) {
        if (sql == null || sql.trim().isEmpty()) return Kind.UNSUPPORTED;
        try {
            CharStream input = new ParseDriver().new ANTLRNoCaseStringStream(sql);
            CommonTokenStream tokens = new CommonTokenStream(new QuietLexer(input));
            tokens.fill();
            // Hive's statement rule excludes the client's optional trailing terminator.
            // Inspect lexer tokens so semicolons inside strings/comments remain untouched.
            for (int i = tokens.size() - 1; i >= 0; i--) {
                Token token = tokens.get(i);
                if (token.getType() == Token.EOF || token.getChannel() != Token.DEFAULT_CHANNEL) continue;
                if (token.getType() == HiveLexer.SEMICOLON) token.setChannel(Token.HIDDEN_CHANNEL);
                break;
            }
            tokens.reset();
            HiveParser parser = new QuietParser(tokens);
            parser.setTreeAdaptor(ParseDriver.adaptor);
            Tree statement = (Tree) parser.statement().getTree();
            // Delegated grammar rules can recover without invoking QuietParser's error handler.
            if (parser.getNumberOfSyntaxErrors() != 0) return Kind.UNSUPPORTED;
            // statement() requires EOF, and its nil root contains the statement and EOF.
            if (statement == null) return Kind.UNSUPPORTED;
            if (statement.isNil()) {
                if (statement.getChildCount() != 2
                        || statement.getChild(1).getType() != Token.EOF) return Kind.UNSUPPORTED;
                statement = statement.getChild(0);
            }
            if (statement.getType() == HiveParser.TOK_CREATETABLE) {
                Tree query = onlyChildOfType(statement, HiveParser.TOK_QUERY);
                return query != null && classifyQuery(query) == Kind.SELECT ? Kind.CTAS : Kind.UNSUPPORTED;
            }
            return statement.getType() == HiveParser.TOK_QUERY
                    ? classifyQuery(statement) : Kind.UNSUPPORTED;
        } catch (RecognitionException | RuntimeException invalidSql) {
            return Kind.UNSUPPORTED;
        }
    }

    private static Kind classifyQuery(Tree query) {
        // Only direct children count: CTEs, subqueries, and UNION inputs have their own sinks.
        Tree insert = onlyChildOfType(query, HiveParser.TOK_INSERT);
        if (insert == null || insert.getChildCount() == 0) return Kind.UNSUPPORTED;
        Tree clause = insert.getChild(0);
        if (clause.getChildCount() == 0) return Kind.UNSUPPORTED;
        Tree destination = clause.getChild(0);
        if (clause.getType() == HiveParser.TOK_INSERT_INTO) {
            return destination.getType() == HiveParser.TOK_TAB ? Kind.INSERT_INTO : Kind.UNSUPPORTED;
        }
        if (clause.getType() != HiveParser.TOK_DESTINATION) return Kind.UNSUPPORTED;
        if (destination.getType() == HiveParser.TOK_TAB) return Kind.INSERT_OVERWRITE;
        if (destination.getType() != HiveParser.TOK_DIR || destination.getChildCount() == 0) {
            return Kind.UNSUPPORTED;
        }
        int pathType = destination.getChild(0).getType();
        if (pathType == HiveParser.TOK_TMP_FILE) return Kind.SELECT;
        return pathType == HiveParser.StringLiteral ? Kind.INSERT_DIRECTORY : Kind.UNSUPPORTED;
    }

    private static Tree onlyChildOfType(Tree parent, int type) {
        Tree match = null;
        for (int i = 0; i < parent.getChildCount(); i++) {
            Tree child = parent.getChild(i);
            if (child.getType() == type) {
                if (match != null) return null;
                match = child;
            }
        }
        return match;
    }

    private static final class QuietLexer extends HiveLexer {
        QuietLexer(CharStream input) { super(input); }

        @Override protected boolean allowQuotedId() {
            // Hive's default is column quoting; no HiveConf/session initialization is needed.
            return true;
        }

        @Override public void displayRecognitionError(String[] tokenNames, RecognitionException error) {
            throw new IllegalArgumentException("Invalid Hive SQL");
        }
    }

    private static final class QuietParser extends HiveParser {
        QuietParser(TokenStream input) { super(input); }

        @Override public void displayRecognitionError(String[] tokenNames, RecognitionException error) {
            throw new IllegalArgumentException("Invalid Hive SQL");
        }
    }
}
