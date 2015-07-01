/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.phoenix;


import com.google.common.collect.Lists;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.QueryUtil;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Phoenix interpreter for Zeppelin. 
 *
 */
public class PhoenixInterpreter extends Interpreter {

  private Logger logger = LoggerFactory.getLogger(PhoenixInterpreter.class);

  public static final String PHOENIX_ZOOKEEPER_QUORUM  = "zookeeper.quorum.uri";
  public static final String PHOENIX_ZOOKEEPER_ZNODE_PARENT  = "zookeeper.znode.parent";
  public static final String PHOENIX_DRIVER_NAME = "org.apache.phoenix.jdbc.PhoenixDriver";
  
  private Exception exceptionOnConnect;
  private Connection connection;
  private Statement statement;

  static {
    Interpreter.register(
        "phoenix",
        "phoenix",
        PhoenixInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
          .add(PHOENIX_ZOOKEEPER_QUORUM, "jdbc:phoenix:localhost", "default connect url.")
          .add(PHOENIX_ZOOKEEPER_ZNODE_PARENT, "/hbase", "default znode parent.")
          .build());
  }
  
  public PhoenixInterpreter(Properties property) {
    super(property);
  }

  public Connection getJdbcConnection() throws SQLException {
    final String zookeeperQuorum = getProperty(PHOENIX_ZOOKEEPER_QUORUM);
    final String zookeeperParent = getProperty(PHOENIX_ZOOKEEPER_ZNODE_PARENT); 
    final Properties props = new Properties(); 
    props.put(PHOENIX_ZOOKEEPER_ZNODE_PARENT, zookeeperParent);
    return DriverManager.getConnection(QueryUtil.getUrl(zookeeperQuorum), props);
  }

  @Override
  public void open() {
    logger.info("Jdbc open connection called!");
    try {
      Class.forName(PHOENIX_DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      logger.error("Can not open connection", e);
      exceptionOnConnect = e;
      return;
    }
    try {
      connection = getJdbcConnection();
      exceptionOnConnect = null;
      logger.info("Successfully created connection");
    } catch (SQLException e) {
      logger.error("Cannot open connection", e);
      exceptionOnConnect = e;
    }
  }

  @Override
  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      logger.error("Cannot close connection", e);
    } finally {
      connection = null;
      exceptionOnConnect = null;
    }
  }

  private InterpreterResult executeSql(String sql) {
    try {
      if (exceptionOnConnect != null) {
        return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
      }
      statement = connection.createStatement();
      StringBuilder response  = new StringBuilder("%table ");
      ResultSet res = statement.executeQuery(sql);
      List<ColumnInfo> columnMetadata = Lists.newArrayListWithExpectedSize(10);
      try {
        ResultSetMetaData md = res.getMetaData();
        int columnCount = md.getColumnCount();
        for (int i = 1; i < columnCount + 1; i++) {
          int sqlType = md.getColumnType(i);
          String columnName = md.getColumnName(i);
          if (i == 1) {
            response.append(columnName);
          } else {
            response.append("\t" + columnName);
          }
          ColumnInfo column = new ColumnInfo(columnName, sqlType);
          columnMetadata.add(column);
        }
        response.append("\n");
        while (res.next()) {
          for (int i = 1; i < columnCount + 1; i++) {
            String value = res.getString(i);
            ColumnInfo cinfo = columnMetadata.get(i - 1);
            Object columnValue = PDataType.fromTypeId(cinfo.getSqlType()).toObject(value);
            response.append(columnValue.toString() + "\t");
          }
          response.append("\n");
        }
      } finally {
        try {
          res.close();
          statement.close();
        } finally {
          statement = null;
        }
      }
      
      InterpreterResult interpreterResult = new InterpreterResult(Code.SUCCESS, 
          response.toString());
      return interpreterResult;
    } catch (SQLException ex) {
      logger.error("Can not run " + sql, ex);
      return new InterpreterResult(Code.ERROR, ex.getMessage());
    }
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
    logger.info("Run SQL command '" + cmd + "'");
    return executeSql(cmd);
  }

  @Override
  public void cancel(InterpreterContext context) {
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
      PhoenixInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }
}
