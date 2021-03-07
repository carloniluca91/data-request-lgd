package it.luca.lgd.impala.client;

import com.cloudera.impala.jdbc.DataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class ImpalaJDBCClient {

    private final Connection connection;

    public ImpalaJDBCClient(String url) throws ClassNotFoundException, SQLException {

        String IMPALA_JDBC_DRIVER = "com.cloudera.impala.jdbc.Driver";
        Class.forName(IMPALA_JDBC_DRIVER);
        DataSource dataSource = new DataSource();
        dataSource.setURL(url);
        connection = dataSource.getConnection();
        log.info("Successfully connected to Impala URL {}", url);
    }

    public void executeStatement(String statement) throws SQLException {

        String preparingMsg = String.format("Preparing to execute provided statement '%s'", statement);
        String executedMsg = String.format("Successfully executed provided statement '%s'", statement);
        executeUpdate(preparingMsg, statement, executedMsg);
    }

    private void executeUpdate(String preparingStatementMsg, String statement, String executedStatementMsg) throws SQLException {

        log.info(preparingStatementMsg);
        connection.createStatement().executeUpdate(statement);
        log.info(executedStatementMsg);
    }

    public void close() throws SQLException {

        connection.close();
        log.info("Successfully closed connection");
    }

    public static String getFQTableName(String db, String table) {

        return String.format("%s.%s", db, table);
    }

    public void invalidateMetadata(String db, String tableName) throws SQLException {

        invalidateMetadata(getFQTableName(db, tableName));
    }

    public void invalidateMetadata(String fqTableName) throws SQLException {

        String preparingMsg = String.format("Preparing to issue invalidate metadata statement on table '%s'", fqTableName);
        String statement = String.format("INVALIDATE METADATA %s", fqTableName);
        String executedMsg = String.format("Successfully issued invalidate metadata statement on table '%s'", fqTableName);
        executeUpdate(preparingMsg, statement, executedMsg);
    }

    public void refreshTable(String db, String tableName) throws SQLException {

        refreshTable(getFQTableName(db, tableName));
    }

    public void refreshTable(String fqTableName) throws SQLException {

        String preparingMsg = String.format("Preparing to issue refresh statement on whole table '%s'", fqTableName);
        String statement = String.format("REFRESH %s", fqTableName);
        String executedMsg = String.format("Successfully issued refresh statement on whole table '%s'", fqTableName);
        executeUpdate(preparingMsg, statement, executedMsg);
    }

    public void refreshTable(String db, String tableName, String partitionColumn, String partitionValue) throws SQLException {

        refreshTable(getFQTableName(db, tableName), partitionColumn, partitionValue);
    }

    public void refreshTable(String fqTableName, String partitionColumn, String partitionValue) throws SQLException {

        String preparingMsg = String.format("Preparing to issue refresh statement on table '%s', partition (%s = %s)",
                fqTableName, partitionColumn, partitionValue);
        String statement = String.format("REFRESH %s PARTITION (%s = %s)", fqTableName, partitionColumn, partitionValue);
        String executedMsg = String.format("Successfully issued refresh statement on table '%s', partition (%s = %s)",
                fqTableName, partitionColumn, partitionValue);
        executeUpdate(preparingMsg, statement, executedMsg);
    }
}

