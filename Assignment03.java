/**
 * Distributed Databases - Assignment 02
 * HS-Fulda SoSe '17
 *
 * @author  Manasés Jesús
 */

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class Assignment03 {
    /* Database objects and credentials */
    private final String URL      = "jdbc:oracle:thin:@mtsthelens.informatik.hs-fulda.de:1521:oralv9a";
    private final String USER     = "VDBSA15";
    private final String PASSWD   = "VDBSA15";

    private final String USER_LAB     = "PROJA14";
    private final String PASSWD_LAB   = "ASSIGN4";

    private Connection connection = null;
    private Statement stmt = null;


    public static void main (String[] args) {
        new Assignment02().performTasks();
    }

    /** Perform all DB tasks
     * */
    protected void performTasks () {
        ResultSet rs = null;
        String SQL = "";
        String column = "";

        System.out.println("Establishing DB connection...");
        try {
            // Establish connection
            connection = DriverManager.getConnection(URL, USER, PASSWD);
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            // Prepare statement
            stmt = connection.createStatement();


            // Tables' names
            String[] tables = { "R1K", "R10K", "R100K", "R1000K" };

            /* 1. SELECT - Full database mode */

            /* 2. SELECT - No database mode */

            /* 3. SELECT - Partial database mode */


        } catch (SQLException ex) {
            ex.printStackTrace();

            /* Catch all SQLExceptions and do rollbacks */
            System.out.println("Rolling back data...");
            try {
                if (connection != null)
                    connection.rollback();
            } catch (SQLException sqle) {
                sqle.printStackTrace();
            }
        }
        finally {
            try {
                /* Only commit after all updates and inserts have been successful */
                if (connection != null)
                    connection.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

}
