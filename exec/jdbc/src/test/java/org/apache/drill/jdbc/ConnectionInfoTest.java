/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.jdbc;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.drill.categories.JdbcTest;
import org.apache.drill.categories.SlowTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Test for Drill's Properties in the JDBC URL connection string
 */
@Category({SlowTest.class, JdbcTest.class})
public class ConnectionInfoTest extends JdbcTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void tearDown() {
    reset();
  }

  @Test
  public void testQuotingIdentifiersProperty() throws Exception {
    Connection connection = connect("jdbc:drill:zk=local;quoting_identifiers='\"'");
    DatabaseMetaData dbmd = connection.getMetaData();
    assertThat(dbmd.getIdentifierQuoteString(), equalTo(Quoting.DOUBLE_QUOTE.string));

    reset();

    connection = connect("jdbc:drill:zk=local;quoting_identifiers=[");
    dbmd = connection.getMetaData();
    assertThat(dbmd.getIdentifierQuoteString(), equalTo(Quoting.BRACKET.string));
  }

  @Test
  public void testIncorrectCharacterForQuotingIdentifiers() throws Exception {
    thrown.expect(SQLException.class);
    thrown.expectMessage(containsString("Option planner.parser.quoting_identifiers must be one of: [`, \", []"));

    connect("jdbc:drill:zk=local;quoting_identifiers=&");
  }

  @Test
  public void testIncorrectValueForSystemOption() throws Exception {
    thrown.expect(SQLException.class);
    thrown.expectMessage(containsString("VALIDATION ERROR: '1' is not a valid value for option 'planner.enable_nljoin" +
      "_for_scalar_only' of type BOOLEAN"));

    connect("jdbc:drill:zk=local;planner.enable_nljoin_for_scalar_only=1");
  }

  @Test
  public void testSessionPropertySet() throws SQLException {
    Connection connection = connect("jdbc:drill:zk=local;planner.enable_nljoin_for_scalar_only=false");
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select * from sys.options where name = 'planner.enable_nljoin_for_scalar_only' and optionScope = 'SESSION' and val = 'false'");
    int resultSize = 0;
    while (resultSet.next()) {
      resultSize++;
    }
    assertEquals("planner.enable_nljoin_for_scalar_only should been set to 'false' at SESSION level ", 1, resultSize);
  }

  @Test
  public void testDontFailOnUnknownProperty() {
    assertDoesNotThrow(() -> {
      connect("jdbc:drill:zk=local;ababagalamaga=true");
    });
  }

  @Test
  public void testOverrideDeprecatedServerPropertyIfSystemPresent() throws SQLException {
    Connection connection = connect("jdbc:drill:zk=local;quoting_identifiers=[;planner.parser.quoting_identifiers='\"';");
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select * from sys.options where name = 'planner.parser.quoting_identifiers' and optionScope = 'SESSION' and val = '\"'");
    int resultSize = 0;
    while (resultSet.next()) {
      resultSize++;
    }
    assertEquals("planner.enable_nljoin_for_scalar_only should been set to '\"' at SESSION level ", 1, resultSize);
  }

  @Test
  public void testSetSchemaUsingConnectionMethod() throws Exception {
    Connection connection = connect("jdbc:drill:zk=local");
    assertNull(connection.getSchema());

    connection.setSchema("dfs.tmp");
    assertEquals("dfs.tmp", connection.getSchema());
  }

  @Test
  public void testIncorrectlySetSchema() throws Exception {
    Connection connection = connect("jdbc:drill:zk=local");

    thrown.expect(SQLException.class);
    thrown.expectMessage("Error when setting schema");

    connection.setSchema("ABC");
  }

  @Test
  public void testSchemaInConnectionString() throws Exception {
    Connection connection = connect("jdbc:drill:zk=local;schema=sys");
    assertEquals("sys", connection.getSchema());
  }

}
