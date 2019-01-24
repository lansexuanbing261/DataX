package com.alibaba.datax.plugin.rdbms.util;

import java.sql.Connection;




/**
 * Date: 15/3/16 下午3:12
 */
public class JdbcConnectionFactory implements ConnectionFactory {

    private DataBaseType dataBaseType;

    private String jdbcUrl;

    private String userName;

    private String password;

    public JdbcConnectionFactory(DataBaseType dataBaseType, String jdbcUrl, String userName, String password) {
        this.dataBaseType = dataBaseType;
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public Connection getConnecttion() {
        String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();

        System.out.println("path: " + path);
        System.out.println("jdbcUrl: " + jdbcUrl);
        System.out.println("userName: " + userName);
        System.out.println("password: " + password);
        System.out.println("begin to get connection....");
        return DBUtil.getConnection(dataBaseType, jdbcUrl, userName, password);
    }

    @Override
    public Connection getConnecttionWithoutRetry() {
        return DBUtil.getConnectionWithoutRetry(dataBaseType, jdbcUrl, userName, password);
    }

    @Override
    public String getConnectionInfo() {
        return "jdbcUrl:" + jdbcUrl;
    }
}
