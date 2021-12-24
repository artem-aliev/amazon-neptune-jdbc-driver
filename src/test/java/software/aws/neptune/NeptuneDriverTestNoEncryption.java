/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.aws.neptune;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class NeptuneDriverTestNoEncryption extends NeptuneDriverTestBase {
    private static final boolean NO_ENCRYPTION = false;

    /**
     * Function to get a random available port and initialize database before testing,
     * with encryption disabled.
     */
    @BeforeAll
    public static void initializeDatabase() {
        initializeDatabase(NO_ENCRYPTION);
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        shutdownTheDatabase();
    }

    @BeforeEach
    void initialize() {
        super.initialize();
    }

    @Test
    void testSQL() throws SQLException {
        String url = "jdbc:neptune:sqlgremlin://127.0.0.1;port=8182;authScheme=None;enableSsl=false";
        Connection connection = DriverManager.getConnection(url);
        //ResultSet result = connection.createStatement().executeQuery("select person.name p, software.name s from person inner join software on person.created_OUT_ID = software.created_IN_ID limit 1");
        //ResultSet result = connection.createStatement().executeQuery("select w_name from bmsql_warehouse where w_tax = 0.2 limit 1");
        ResultSet result = connection.createStatement().executeQuery("select * from person");
        result.next();
        System.out.println("-----------------------------------------");
        for(int i =1; i <=result.getMetaData().getColumnCount();i++){
            System.out.print(" " +result.getMetaData().getColumnName(i));
        }
        System.out.println("");
        System.out.println("-----------------------------------------");

        do {
            for(int i =1; i <=result.getMetaData().getColumnCount();i++){
                System.out.print(" " +result.getObject(i));
            }
            System.out.println("");
        } while (result.next());
        System.out.println("-----------------------------------------");

    }

    @Test
    void testAcceptsUrl() throws SQLException {
        super.testAcceptsUrl(NO_ENCRYPTION);
    }

    @Test
    void testConnect() throws SQLException {
        super.testConnect(NO_ENCRYPTION);
    }

    @Test
    void testDriverManagerGetConnection() throws SQLException {
        super.testDriverManagerGetConnection(NO_ENCRYPTION);
    }

    @Test
    void testDriverManagerGetDriver() throws SQLException {
        super.testDriverManagerGetDriver(NO_ENCRYPTION);
    }
}
