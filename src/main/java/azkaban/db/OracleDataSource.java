//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package azkaban.db;

import azkaban.utils.Props;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.inject.Inject;
import org.apache.log4j.Logger;

public class OracleDataSource extends AzkabanDataSource {
    private static final Logger logger = Logger.getLogger(OracleDataSource.class);
    private final DBMetrics dbMetrics;
    private String url2;
    private String uploadMode;

    @Inject
    public OracleDataSource(Props props, DBMetrics dbMetrics) {
        this.dbMetrics = dbMetrics;
        int port = props.getInt("oracle.port");
        String host = props.getString("oracle.host");
        String dbName = props.getString("oracle.database");
        String user = props.getString("oracle.user");
        String password = props.getString("oracle.password");
        int numConnections = props.getInt("oracle.numconnections");
        //String url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName;
        String url = props.getString("oracle.url");
        String driver = props.getString("oracle.driver");
        //String alternateUrl = "jdbc:oracle:thin:@" + host + ":" + port + "/" + dbName;
        this.addConnectionProperty("useUnicode", "yes");
        this.addConnectionProperty("characterEncoding", "UTF-8");
        //this.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        this.setDriverClassName(driver);
        this.setUsername(user);
        this.setPassword(password);
        this.setUrl(url);
        //this.setUrl2(alternateUrl);
        this.setMaxTotal(numConnections);
        this.setValidationQuery("/* ping */ select 1");
        this.setTestOnBorrow(true);
    }

    @Override
    public Connection getConnection() throws SQLException {
        this.dbMetrics.markDBConnection();
        long startMs = System.currentTimeMillis();
        Connection connection = null;
        boolean var4 = true;

        try {
            logger.info("Getting connection to Oracle DB using connection string : " + this.getUrl());
            connection = DriverManager.getConnection(this.getUrl(), this.getUsername(), this.getPassword());
            logger.info("******* Oracle Connection Successful ********* : " + this.getUrl());
        } catch (Exception var8) {
            logger.error("Failed in getting connection to Oracle DB using connection string : " + this.getUrl());
            logger.error(var8);
            throw new SQLException("Failed in getting connection to Oracle DB", var8);

//            try {
//                logger.info("Trying to get connection to Oracle DB using connection string : " + this.getUrl2());
//                connection = DriverManager.getConnection(this.getUrl2(), this.getUsername(), this.getPassword());
//                logger.info("******* Oracle Connection Successful ********* : "+ this.getUrl2());
//            } catch (Exception var7) {
//                logger.error("Failed in getting connection to Oracle DB");
//                throw new SQLException("Failed in getting connection to Oracle DB", var7);
//            }
        }

        return connection;
    }

    private boolean isReadOnly(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT open_mode FROM v$database");
        if (rs.next()) {
            String value = rs.getString(1);
            return value.equalsIgnoreCase("READ ONLY");
        } else {
            throw new SQLException("can not fetch read only value from DB");
        }
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException var4) {
            logger.error("Sleep interrupted", var4);
        }

    }

    @Override
    public String getDBType() {
        return "oracle";
    }

    @Override
    public boolean allowsOnDuplicateKey() {
        return true;
    }

    public String getUrl2() {
        return this.url2;
    }

    public void setUrl2(String url2) {
        this.url2 = url2;
    }
}