package com.abm.pers_finance;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
*This class managers the creation of JDBC connections to the database.
*/
public final class DBConnectionManager {

/**
*This is a private constructor to prevent subclassing of this singleton class.
*/
    private DBConnectionManager() { }


/**
*This is a public helper method that returns JDBC connections to the
 * postgresql database on the localhost.
*@param username the database username
*@param password the database password
 * @return JDBC connection to postgresql database.
*/
    public static Connection getDBConnection(final String username, final String password) {

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/postgres", username,
                    password);

            return connection;

        } catch (SQLException e) {
            e.printStackTrace();
            return connection;

       }

    }

}





