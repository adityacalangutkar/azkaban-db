/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package azkaban.db;

import java.sql.*;
import java.util.Arrays;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.log4j.Logger;


/**
 * This interface is designed as an supplement of {@link DatabaseOperator}, which do commit at the
 * end of every query. Given this interface, users/callers (implementation code) should decide where
 * to {@link Connection#commit()} based on their requirements.
 *
 * The diff between DatabaseTransOperator and DatabaseOperator: * Auto commit and Auto close
 * connection are enforced in DatabaseOperator, but not enabled in DatabaseTransOperator. * We
 * usually group a couple of sql operations which need the same connection into
 * DatabaseTransOperator.
 *
 * @see org.apache.commons.dbutils.QueryRunner
 */
public class DatabaseTransOperator {

  private static final Logger logger = Logger.getLogger(DatabaseTransOperator.class);
  private final Connection conn;
  private final QueryRunner queryRunner;

  public DatabaseTransOperator(final QueryRunner queryRunner, final Connection conn) {
    this.conn = conn;
    this.queryRunner = queryRunner;
  }

  /**
   * returns the last id from a previous insert statement. Note that last insert and this operation
   * should use the same connection.
   *
   * @return the last inserted id in mysql per connection.
   */
  public long getLastInsertId() throws SQLException {
    // A default connection: autocommit = true.
    long num = -1;
    try {
      num = ((Number) this.queryRunner
          .query(this.conn, "SELECT LAST_INSERT_ID();", new ScalarHandler<>(1)))
          .longValue();
    } catch (final SQLException ex) {
      logger.error("can not get last insertion ID");
      throw ex;
    }
    return num;
  }

  public long setAndGetLastInsertIdFromOracle(long lastAutoIncrId) throws SQLException{
    return lastAutoIncrId;
  }

  /**
   *
   * @param querySql
   * @param resultHandler
   * @param params
   * @param <T>
   * @return
   * @throws SQLException
   */
  public <T> T query(final String querySql, final ResultSetHandler<T> resultHandler,
      final Object... params)
      throws SQLException {
    try {
      return this.queryRunner.query(this.conn, querySql, resultHandler, params);
    } catch (final SQLException ex) {
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }

  /**
   *
   * @param updateClause
   * @param params
   * @return
   * @throws SQLException
   */
  public int update(final String updateClause, final Object... params) throws SQLException {
    try {
      return this.queryRunner.update(this.conn, updateClause, params);
    } catch (final SQLException ex) {
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }

  public long updateAndGetLastAutoIncrId(String updateClause,String tableName,String autoIncrColName,Object... params) throws SQLException{
    logger.info("updateClause : "+ updateClause);
    logger.info("params : " + Arrays.toString(params));
    long id = -1;
    try{
      String generatedColumns[]={"exec_id"};
      PreparedStatement ps = this.conn.prepareStatement(updateClause,generatedColumns);
      //fillStatement(ps, params);
      //
      for (int i = 0; i < params.length; i++) {
        if (params[i] != null) {
          ps.setObject(i + 1, params[i]);
        }
      }
      //
      try {
        logger.info("Using Connection to get exec_id : " + this.conn.getMetaData().getURL());
      }catch (Exception e){
        logger.error("Error getting connection object details. " + e);
      }
      logger.info("Prepared Statement : " + ps);
      int rows = ps.executeUpdate();
      logger.info("Number of Rows updated : " + rows);
      ResultSet generatedKeys = ps.getGeneratedKeys();
      //generatedKeys.
      while(generatedKeys.next()){
        id = Long.valueOf(generatedKeys.getInt(1));
        logger.info("updateAndGetLastAutoIncrId : " + id);
      }

      /*this.queryRunner.update(this.conn,updateClause,params);
      ScalarHandler<Number> scalarHandler = new ScalarHandler<>();
      Number number=(Number) this.queryRunner.query(this.conn,"SELECT MAX("+autoIncrColName+" ) FROM "+ tableName,scalarHandler);
      long var7 = number.longValue();
      logger.info("***** SELECT MAX("+autoIncrColName+" ) FROM "+ tableName + " returns value : " + var7);
      return var7;*/
      return id;
    }catch (SQLException ex){
      logger.error("Error in updateAndGetLastAutoIncrId "+ ex);
      throw ex;
    }finally {

    }
  }


  /**
   * Fill the <code>PreparedStatement</code> replacement parameters with the
   * given objects.
   *
   * @param stmt
   *            PreparedStatement to fill
   * @param params
   *            Query replacement parameters; <code>null</code> is a valid
   *            value to pass in.
   * @throws SQLException
   *             if a database access error occurs
   */
  public PreparedStatement fillStatement(PreparedStatement stmt, Object... params)
          throws SQLException {
    boolean pmdKnownBroken = false;
    // check the parameter count, if we can
    ParameterMetaData pmd = stmt.getParameterMetaData();
      int stmtCount = pmd.getParameterCount();
      int paramsCount = params == null ? 0 : params.length;

      if (stmtCount != paramsCount) {
        throw new SQLException("Wrong number of parameters: expected "
                + stmtCount + ", was given " + paramsCount);
      }

    // nothing to do here
    if (params == null) {
      return stmt;
    }

    for (int i = 0; i < params.length; i++) {
      if (params[i] != null) {
        stmt.setObject(i + 1, params[i]);
      } else {
        // VARCHAR works with many drivers regardless
        // of the actual column type. Oddly, NULL and
        // OTHER don't work with Oracle's drivers.
        int sqlType = Types.VARCHAR;
        if (!pmdKnownBroken) {
          try {
            /*
             * It's not possible for pmdKnownBroken to change from
             * true to false, (once true, always true) so pmd cannot
             * be null here.
             */
            sqlType = pmd.getParameterType(i + 1);
          } catch (SQLException e) {
            pmdKnownBroken = true;
          }
        }
        stmt.setNull(i + 1, sqlType);
      }
    }
    return stmt;
  }

  /**
   * @return the JDBC connection associated with this operator.
   */
  public Connection getConnection() {
    return this.conn;
  }
}
