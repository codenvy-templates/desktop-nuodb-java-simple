package com.codenvy.example.nuodb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Application {
    private final Connection dbConnection;

    /**
     * Creates an instance of HelloDB and connects to a local server,
     * as the given user, to work with the given named database
     *
     * @param user
     *         the user name for the connection
     * @param password
     *         the password for the given user
     * @param dbName
     *         the name of the database at the server to use
     */
    public Application(String user, String password, String dbName) throws SQLException {
        Properties properties = new Properties();
        properties.put("user", user);
        properties.put("password", password);
        properties.put("schema", "testSchema");

        dbConnection = DriverManager.getConnection(String.format("jdbc:com.nuodb://127.0.0.1/%s", dbName), properties);
    }

    /** Closes the connection to the server. */
    public void close() throws SQLException {
        dbConnection.close();
    }

    /** Creates a simple two-column table: id->name. */
    public void createNameTable() throws SQLException {
        try (Statement statement = dbConnection.createStatement()) {
            statement.execute("create table names (id int primary key, name string)");
            statement.close();
            dbConnection.commit();
        } catch (Exception exception) {
            System.out.println("Skipping table creation: " + exception.getMessage());
        }
    }

    /**
     * Inserts a row into the table. The id must be unique.
     *
     * @param id
     *         a unique numeric identifier
     * @param name
     *         a name associated with the given id
     */
    public void insertName(int id, String name) throws SQLException {
        try (PreparedStatement statement = dbConnection.prepareStatement("insert into names (id, name) values (?, ?)")) {
            statement.setInt(1, id);
            statement.setString(2, name);
            statement.addBatch();
            statement.executeBatch();
            dbConnection.commit();
        } catch (Exception exception) {
            System.out.println("Skipping insert...");
        }
    }

    /**
     * Gets the name for the given id, or null if no name exists.
     *
     * @param id
     *         identifier
     * @return the name associate with the identifier, or null
     */
    public String getName(int id) throws SQLException {
        try (Statement statement = dbConnection.createStatement();
             ResultSet resultSet = statement.executeQuery(String.format("select name from names where id=%d", id))) {
            if (resultSet.next())
                return resultSet.getString(1);
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        Class.forName("com.nuodb.jdbc.Driver");

        Application application = new Application("dba", "bird", "testDB");
        application.createNameTable();
        application.insertName(12, "fred");
        System.out.println("Name for ID=12 is: " + application.getName(12));

        application.close();
    }
}
