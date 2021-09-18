package com.github.kylin.datasource;

import lombok.extern.slf4j.Slf4j;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-04-19 14:52
 * @description kylin jdbc data source.
 */
@Slf4j
public class KylinDataSource implements DataSource{
    private final LinkedList<Connection> connectionPoolList = new LinkedList<>();
    private long maxWaitTime;

    public KylinDataSource(KylinDataSourceProperties kylinProperties) {
        try {
            this.maxWaitTime = kylinProperties.getMaxWaitTime();
            Driver driverManager = (Driver) Class.forName(kylinProperties.getDriverClassName()).newInstance();
            Properties prop = new Properties();
            prop.put("user", kylinProperties.getUsername());
            prop.put("password", kylinProperties.getPassword());
            for (int i = 0; i < kylinProperties.getPoolSize(); i++) {
                Connection connection = driverManager.connect(kylinProperties.getJdbcUrl(), prop);
                connectionPoolList.add(ConnectionProxy.getProxy(connection, connectionPoolList));
            }
            log.info("KylinDataSource has initialized {} size connection pool:", connectionPoolList.size());
        } catch (Exception ex) {
            log.error("kylinDataSource initialize error, ex:", ex);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        synchronized (connectionPoolList) {
            if (connectionPoolList.size() <= 0) {
                try {
                    connectionPoolList.wait(maxWaitTime);
                } catch (InterruptedException e) {
                    throw new SQLException("getConnection timeout..." + e.getMessage());
                }
            }
            return connectionPoolList.removeFirst();
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    /**
     * 连接代理.
     * */
    private static class ConnectionProxy implements InvocationHandler {
        private Object obj;
        private final LinkedList<Connection> pool;

        private ConnectionProxy(Object obj, LinkedList<Connection> pool) {
            this.obj = obj;
            this.pool = pool;
        }

        private static Connection getProxy(Object obj, LinkedList<Connection> pool) {
            Object proxyInstance = Proxy.newProxyInstance(
                    obj.getClass().getClassLoader(),
                    new Class[]{Connection.class},
                    new ConnectionProxy(obj, pool));
            return (Connection) proxyInstance;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) {
                synchronized (pool) {
                    pool.add((Connection) proxy);
                    pool.notify();
                }
                return null;
            } else {
                return method.invoke(obj, args);
            }
        }
    }
}
