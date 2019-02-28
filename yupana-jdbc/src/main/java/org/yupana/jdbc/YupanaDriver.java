package org.yupana.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This class should be implemented in Java to have static initializer in the same class
 */
public class YupanaDriver implements Driver {

    private static final String URL_PREFIX = "jdbc:yupana";

    static {
        try {
            DriverManager.registerDriver(new YupanaDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Init failed: " + e.getMessage());
        }
    }

    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        Properties urlProps = parseUrl(url);
        Properties p = new Properties();
        p.putAll(urlProps);
        p.putAll(properties);
        try {
            return new YupanaConnection(url, p);
        } catch (Throwable t) {
            throw new SQLException("Yupana connection failed", t);
        }

    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported: Driver.getParentLogger()");
    }

    private Properties parseUrl(String url) throws SQLException {
        Properties result = new Properties();
        if (url != null && url.startsWith(URL_PREFIX)) {
            try {
                URI uri = new URI(url.substring(5));
                result.put("yupana.host", uri.getHost());
                if (uri.getPort() != -1) {
                    result.put("yupana.port", String.valueOf(uri.getPort()));
                }
            } catch (URISyntaxException e) {
                throw new SQLException("Invalid URL " + url, e);
            }
        }

        return result;
    }
}
