package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.table.MySqlTableSourceFactory;
import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.databases.mysql.dialect.MySqlDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.Factory;

import java.util.List;
import java.util.Optional;

public class CdcCatalog extends AbstractJdbcCatalog {

    private final AbstractJdbcCatalog internal;

    /**
     * Creates a JdbcCatalog.
     *
     * @deprecated please use {@link JdbcCatalog#JdbcCatalog(ClassLoader, String, String, String,
     *     String, String, String)} instead.
     */
    public CdcCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String ip,
            String port) {
        this(
                Thread.currentThread().getContextClassLoader(),
                catalogName,
                defaultDatabase,
                username,
                pwd,
                ip,
                port,
                null);
    }

    /**
     * Creates a JdbcCatalog.
     *
     * @param userClassLoader the classloader used to load JDBC driver
     * @param catalogName the registered catalog name
     * @param defaultDatabase the default database name
     * @param username the username used to connect the database
     * @param pwd the password used to connect the database
     * @param baseUrl the base URL of the database, e.g. jdbc:mysql://localhost:3306
     * @param compatibleMode the compatible mode of the database
     */
    public CdcCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String ip,
            String port,
            String compatibleMode) {
        super(userClassLoader, catalogName, defaultDatabase, username, pwd, "jdbc:mysql://"+ip+":"+ port+"/");
        JdbcDialect dialect = JdbcDialectLoader.load(baseUrl, compatibleMode, userClassLoader);
        if (dialect instanceof MySqlDialect) {
            internal =  new MySqlCdcCatalog(
                    userClassLoader, catalogName, defaultDatabase, username, pwd, ip, port);
        } else {
            throw new RuntimeException("找不到catalog");
        }
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new MySqlTableSourceFactory());
    }


    // ------ databases -----

    @Override
    public List<String> listDatabases() throws CatalogException {
        return internal.listDatabases();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return internal.getDatabase(databaseName);
    }

    // ------ tables and views ------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return internal.listTables(databaseName);
    }


    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    // ------ getters ------

    @VisibleForTesting
    @Internal
    public AbstractJdbcCatalog getInternal() {
        return internal;
    }



    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return internal.getTable(tablePath);
    }

}
