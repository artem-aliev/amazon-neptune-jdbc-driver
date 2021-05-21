/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.sparql;

import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.riot.web.HttpOp;
import org.apache.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import java.sql.SQLException;
import java.util.Properties;

public class SparqlConnectionPropertiesTest {
    private static final String HOSTNAME = "http://localhost";
    private static final int PORT = 8182;
    private static final String ENDPOINT = "mock";
    private SparqlConnectionProperties connectionProperties;
    private int randomIntValue;

    protected void assertDoesNotThrowOnNewConnectionProperties(final Properties properties) {
        Assertions.assertDoesNotThrow(() -> {
            connectionProperties = new SparqlConnectionProperties(properties);
        });
    }

    protected void assertThrowsOnNewConnectionProperties(final Properties properties) {
        Assertions.assertThrows(SQLException.class,
                () -> connectionProperties = new SparqlConnectionProperties(properties));
    }

    // set the DESTINATION properties properly to avoid throws on tests not related to the exception
    private void setInitialDestinationProperty(final SparqlConnectionProperties connectionProperties)
            throws SQLException {
        connectionProperties.setContactPoint(HOSTNAME);
        connectionProperties.setPort(PORT);
        connectionProperties.setEndpoint(ENDPOINT);
    }

    @BeforeEach
    void beforeEach() {
        randomIntValue = HelperFunctions.randomPositiveIntValue(1000);
    }

    @Test
    void testDefaultValues() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertEquals("", connectionProperties.getEndpoint());
        Assertions.assertEquals(SparqlConnectionProperties.DEFAULT_LOG_LEVEL, connectionProperties.getLogLevel());
        Assertions.assertEquals(SparqlConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS,
                connectionProperties.getConnectionTimeoutMillis());
        Assertions.assertEquals(SparqlConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT,
                connectionProperties.getConnectionRetryCount());
        Assertions
                .assertEquals(SparqlConnectionProperties.DEFAULT_AUTH_SCHEME, connectionProperties.getAuthScheme());
        Assertions.assertEquals("", connectionProperties.getRegion());
    }

    @Test
    void testApplicationName() throws SQLException {
        final String testValue = "test application name";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setApplicationName(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getApplicationName());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testLogLevel() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setLogLevel(Level.ERROR);
        Assertions.assertEquals(Level.ERROR, connectionProperties.getLogLevel());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testConnectionTimeout() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setConnectionTimeoutMillis(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getConnectionTimeoutMillis());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testConnectionRetryCount() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setConnectionRetryCount(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getConnectionRetryCount());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testContactPoint() throws SQLException {
        final String testValue = "test contact point";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setContactPoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getContactPoint());

        // will throw because only ContactPoint is set
        assertThrowsOnNewConnectionProperties(connectionProperties);

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testEndpoint() throws SQLException {
        final String testValue = "test endpoint";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setEndpoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getEndpoint());

        // will throw because only Endpoint is set
        assertThrowsOnNewConnectionProperties(connectionProperties);

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testQueryEndpoint() throws SQLException {
        final String testValue = "test query endpoint";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setQueryEndpoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getQueryEndpoint());

        // will throw because only QueryEndpoint is set
        assertThrowsOnNewConnectionProperties(connectionProperties);

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testPort() throws SQLException {
        final int testValue = 12345;
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setPort(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getPort());

        // will throw because only Port is set
        assertThrowsOnNewConnectionProperties(connectionProperties);

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testAuthScheme() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAuthScheme(AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());
        System.out.println("region is: " + connectionProperties.getRegion());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testRegion() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();

        connectionProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set to None
        final String testValue = "test region";
        connectionProperties.setRegion(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getRegion());

        connectionProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set to IAMSigV4
        final String serviceRegion = System.getenv().get("SERVICE_REGION");
        Assertions.assertNotNull(serviceRegion);
        connectionProperties.setRegion(serviceRegion);
        Assertions.assertEquals(serviceRegion, connectionProperties.getRegion());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testConcurrentModificationExceptionHttpClient() throws SQLException {
        final HttpClient testClient = HttpOp.createDefaultHttpClient();
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertNull(connectionProperties.getHttpClient());

        connectionProperties.setHttpClient(testClient);
        Assertions.assertEquals(testClient, connectionProperties.getHttpClient());

        connectionProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testHttpClient() throws SQLException {
        final HttpClient testClient = HttpOp.createDefaultHttpClient();
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertNull(connectionProperties.getHttpClient());

        connectionProperties.setHttpClient(testClient);
        Assertions.assertEquals(testClient, connectionProperties.getHttpClient());

        connectionProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());

        final Properties testProperties = new Properties();
        testProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
        testProperties.put(SparqlConnectionProperties.CONTACT_POINT_KEY, "http://localhost");
        testProperties.put(SparqlConnectionProperties.PORT_KEY, 8182);
        testProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, "mock");
        testProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "query");
        testProperties.put(SparqlConnectionProperties.HTTP_CLIENT_KEY, testClient);
        Assertions.assertEquals(testClient, testProperties.get(SparqlConnectionProperties.HTTP_CLIENT_KEY));

        assertDoesNotThrowOnNewConnectionProperties(testProperties);
    }

    @Test
    void testHttpClientWithSigV4Auth() {
        final HttpClient testClient = HttpOp.createDefaultHttpClient();
        Assertions.assertNotNull(testClient);

        final Properties testProperties = new Properties();
        testProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4);
        testProperties.put(SparqlConnectionProperties.CONTACT_POINT_KEY, "http://localhost");
        testProperties.put(SparqlConnectionProperties.PORT_KEY, 8182);
        testProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, "mock");
        testProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "query");
        testProperties.put(SparqlConnectionProperties.HTTP_CLIENT_KEY, testClient);
        Assertions.assertEquals(testClient, testProperties.get(SparqlConnectionProperties.HTTP_CLIENT_KEY));

        assertThrowsOnNewConnectionProperties(testProperties);
    }

    @Test
    void testHttpContext() throws SQLException {
        final HttpContext testContext = new HttpClientContext();
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertNull(connectionProperties.getHttpContext());
        connectionProperties.setHttpContext(testContext);
        Assertions.assertEquals(testContext, connectionProperties.getHttpContext());

        final Properties testProperties = new Properties();
        testProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
        testProperties.put(SparqlConnectionProperties.CONTACT_POINT_KEY, "http://localhost");
        testProperties.put(SparqlConnectionProperties.PORT_KEY, 8182);
        testProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, "mock");
        testProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "query");
        testProperties.put(SparqlConnectionProperties.HTTP_CONTEXT_KEY, testContext);
        Assertions.assertEquals(testContext, testProperties.get(SparqlConnectionProperties.HTTP_CONTEXT_KEY));

        assertDoesNotThrowOnNewConnectionProperties(testProperties);
    }

    @Test
    void testAcceptHeaderAskQuery() throws SQLException {
        final String testValue = "test accept header ask query";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderAskQuery(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderAskQuery());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testAcceptHeaderDataset() throws SQLException {
        final String testValue = "test accept header graph";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderDataset(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderDataset());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testAcceptHeaderGraph() throws SQLException {
        final String testValue = "test accept header graph";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderGraph(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderGraph());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testAcceptHeaderQuery() throws SQLException {
        final String testValue = "test accept header query";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderQuery(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderQuery());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testAcceptHeaderSelectQuery() throws SQLException {
        final String testValue = "test accept header select query";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderSelectQuery(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderSelectQuery());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testGspEndpoint() throws SQLException {
        final String testValue = "test gsp endpoint";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setGspEndpoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getGspEndpoint());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testParseCheckSparql() throws SQLException {
        final boolean testValue = true;
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setParseCheckSparql(testValue);
        Assertions.assertTrue(connectionProperties.getParseCheckSparql());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testQuadsFormat() throws SQLException {
        final String testValue = "test quads format";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setQuadsFormat(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getQuadsFormat());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }


    @Test
    void testTriplesFormat() throws SQLException {
        final String testValue = "test triples format";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setTriplesFormat(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getTriplesFormat());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }
}
