package software.amazon.neptune.sparql;

import lombok.SneakyThrows;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.QueryExecutor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SparqlQueryExecutor extends QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlQueryExecutor.class);
    // private static SparqlConnectionProperties previousSparqlConnectionProperties = null;
    private final SparqlConnectionProperties sparqlConnectionProperties;

    SparqlQueryExecutor(final SparqlConnectionProperties sparqlConnectionProperties) {
        this.sparqlConnectionProperties = sparqlConnectionProperties;
    }

    private static RDFConnectionRemoteBuilder createRDFBuilder(final SparqlConnectionProperties properties)
            throws SQLException {
        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create();

        // This is mimicking urlDataset() function in MockServer, the input into builder.destination
        // with returning format: "http://localhost:"+port()+datasetPath()
        // Right now it is being concatenated from various connection properties
        // TODO: Maybe turn databaseUrl into a connection property itself?
        if (properties.containsKey(SparqlConnectionProperties.CONTACT_POINT_KEY) &&
                properties.containsKey(SparqlConnectionProperties.PORT_KEY) &&
                properties.containsKey(SparqlConnectionProperties.ENDPOINT_KEY)) {
            final String databaseUrl = properties.getContactPoint() + properties.getPort() + properties.getEndpoint();
            builder.destination(databaseUrl);
        }

        if (properties.containsKey(SparqlConnectionProperties.QUERY_ENDPOINT_KEY)) {
            builder.queryEndpoint(properties.getQueryEndpoint());
        }

        return builder;
    }

    @Override
    public int getMaxFetchSize() {
        return 0;
    }

    /**
     * Verify that connection to database is functional.
     *
     * @param timeout Time in seconds to wait for the database operation used to validate the connection to complete.
     * @return true if the connection is valid, otherwise false.
     */
    @Override
    @SneakyThrows
    public boolean isValid(final int timeout) {
        final RDFConnection tempConn =
                SparqlQueryExecutor.createRDFBuilder(sparqlConnectionProperties).build();

        try {
            final QueryExecution executeQuery = tempConn.query("SELECT * { ?s ?p ?o } LIMIT 0");
            executeQuery.setTimeout(timeout * 1000);
            executeQuery.execSelect();
            return true;
        } catch (final Exception e) {
            LOGGER.error("Connection to database returned an error:", e);
            return false;
        }
    }

    /**
     * Function to execute query.
     *
     * @param sparql    Query to execute.
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public ResultSet executeQuery(final String sparql, final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get tables.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param tableName String table name with colon delimits.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public ResultSet executeGetTables(final Statement statement, final String tableName) throws SQLException {
        return null;
    }

    /**
     * Function to get schema.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResulSet Object containing schemas.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public ResultSet executeGetSchemas(final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get catalogs.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing catalogs.
     */
    @Override
    public ResultSet executeGetCatalogs(final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing table types.
     */
    @Override
    public ResultSet executeGetTableTypes(final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param nodes     String containing nodes to get schema for.
     * @return java.sql.ResultSet Object containing columns.
     */
    @Override
    public ResultSet executeGetColumns(final Statement statement, final String nodes) throws SQLException {
        return null;
    }

    @Override
    protected <T> T runQuery(final String query) throws SQLException {
        return null;
    }

    @Override
    protected void performCancel() throws SQLException {

    }
}
