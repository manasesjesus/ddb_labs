/**
 * Distributed Databases - Assignment 01
 * HS-Fulda SoSe 17
 *
 * @author  Manasés Jesús
 */

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/** Assignment Rules
 *
 *  R01 - Set autocommit mode to false
 *  R02 - Set isolation level of the transaction to serializable
 *  R03 - Catch all SQLExceptions and do rollback
 *  R04 - Only commit after all updates and inserts have been successful
 * */
public class Assignment01 {

    /* Database objects and credentials */
    static final String URL        = "jdbc:oracle:thin:@__server__.hs-fulda.de:1521:__db-name__";
    static final String USER       = "__REPLACE__";
    static final String PASSWD     = "__REPLACE__";

    static final String USER_LAB   = "__REPLACE__";
    static final String PASSWD_LAB = "__REPLACE__";

    static Statement stmt = null;


    public static void main (String[] args) {

        Connection connection = null;
        ResultSet resultSet = null;
        String SQL = null;
        boolean doSQL = true;

        try {
            // Register driver
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());

            // Open connection (R01, R02)
            connection = DriverManager.getConnection(URL, USER_LAB, PASSWD_LAB);
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            // Prepare statement
            stmt = connection.createStatement();


            /******* 1. Add yourself as a passenger to the database *******/
            if (executeQuery("*", "PASSENGER", "PNO = 30")) {
                stmt.executeUpdate("INSERT INTO PASSENGER (PNO, LASTNAME, FIRSTNAME, COUNTRY) "
                        + "VALUES (30, 'Galindo Bello', 'Manases Jesus', 'MX')");
                System.out.println("A new Passenger was registered successfully!");
            }


            /******* 2. Store 2 airports that are not in the DB *******/
            if (executeQuery("*", "AIRPORT", "COUNTRY = 'MX'")) {
                stmt.executeUpdate("INSERT INTO AIRPORT (APC, COUNTRY, CITY, NAME) VALUES " +
                        "('MEX', 'MX', 'Ciudad de Mexico', 'Benito Juarez')");
                stmt.executeUpdate("INSERT INTO AIRPORT (APC, COUNTRY, CITY, NAME) VALUES " +
                        "('GDL', 'MX', 'Guadalajara', 'Miguel Hidalgo')");
                System.out.println("Airports registered successfully!");
            }


            /******* 3. Insert one airline that is not in the DB yet *******/
            if (executeQuery("*", "AIRLINE", "COUNTRY = 'MX'")) {
                stmt.executeUpdate("INSERT INTO AIRLINE (ALC, COUNTRY, HUB, NAME, ALLIANCE) VALUES " +
                        "('AM', 'MX', 'MEX', 'AeroMexico', 'Star')");
                System.out.println("Airlines registered successfully!");
            }


            /******* 4. Add flights To and Form both "your" airports operated by "your" airline to Frankfurt (FRA). *******/
            if (executeQuery("*", "FLIGHT", "ALC = 'AM'")) {
                stmt.executeUpdate("INSERT INTO FLIGHT VALUES ('AM', '777', '100', 'B777')");
                stmt.executeUpdate("INSERT INTO FLIGHT VALUES ('AM', '019', '100', 'B777')");
                stmt.executeUpdate("INSERT INTO FLIGHT VALUES ('AM', '123', '100', 'B777')");
                stmt.executeUpdate("INSERT INTO FLIGHT VALUES ('AM', '119', '100', 'B777')");
                // Guadalajara > Frankfurt
                stmt.executeUpdate("INSERT INTO LEG VALUES ('AM', '777', 'GDL', 'FRA', '21.30', '13.10')");
                stmt.executeUpdate("INSERT INTO LEG VALUES ('AM', '123', 'FRA', 'GDL', '13.10', '21.30')");
                // Ciudad de Mexico > Frankfurt
                stmt.executeUpdate("INSERT INTO LEG VALUES ('AM', '019', 'MEX', 'FRA', '22.30', '10.30')");
                stmt.executeUpdate("INSERT INTO LEG VALUES ('AM', '119', 'FRA', 'MEX', '10.30', '22.30')");
                System.out.println("Flights registered successfully!");
            }


            /******* 5. Make a booking from "your" first airport via FRA to your second airport.
             *          The day of the booking should be today plus 30 days. In SQL: SYSDATE + 30 *******/
            if (executeQuery("*", "BOOKING", "ALC = 'AM'")) {
                stmt.executeUpdate("INSERT INTO BOOKING VALUES (30, 'AM', 777, 'GDL', 'FRA', SYSDATE+30, 1234, 1200)");
                stmt.executeUpdate("INSERT INTO BOOKING VALUES (30, 'AM', 123, 'FRA', 'GDL', SYSDATE+30, 1234, 1000)");
                System.out.println("Bookings registered successfully!");
            }



            /******* 6. Make a booking from "your" first airport via FRA to JFK.
             *          The day of the booking is today plus 40 days *******/
            if (executeQuery("*", "BOOKING", "ALC = 'AM'")) {
                SQL = "INSERT INTO BOOKING VALUES (30, 'AM', 777, 'GDL', 'FRA', SYSDATE+40, 1234, 1200)";
                stmt.executeUpdate(SQL);
                SQL = "INSERT INTO BOOKING VALUES (30, 'DL', 9, 'FRA', 'JFK', SYSDATE+40, 3900, 2000)";
                stmt.executeUpdate(SQL);
                System.out.println("Bookings registered successfully!");
            }



            /******* 7. Write a query that searches for all flights (nonstop or one-stop and transfer)
             *          from an arbitrary airport X to an arbitrary airport Y and print the flights/legs  *******/
            String a1 = "'TXL'";
            String a2 = "'JFK'";

            if (executeQuery("*", "LEG", "ORIGIN = " + a1 + " AND DESTINATION = " + a2, true)) {

                /******* 8. If no direct flights are found, search for one connection flight *******/
                executeQuery("*", "LEG L1, LEG L2", "L1.ORIGIN = " + a1 + " AND L2.DESTINATION = " + a2 +
                        " AND L1.DESTINATION = L2.ORIGIN", true);
            }

        } catch (SQLException ex) {     /******* R03 *******/
            ex.printStackTrace();

            System.out.println("Rolling back data...");
            try {
                if (connection != null)
                    connection.rollback();
            } catch (SQLException sqle) {
                sqle.printStackTrace();
            }
        }
        finally {
            try {                       /******* R04 *******/
                connection.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }


    /* Database Query Functions */
    private static boolean executeQuery (String value, String table) throws SQLException {
        return executeQuery(value, table, null, false);
    }

    private static boolean executeQuery (String value, String table, String condition) throws SQLException {
        return executeQuery(value, table, condition, false);
    }

    private static boolean executeQuery (String value, String table, String condition, boolean show_labels)
            throws SQLException {
        String SQL   = "SELECT " + value + " FROM " + table + (condition != null ? " WHERE " + condition : "");
        ResultSet rs = stmt.executeQuery(SQL);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columns  = rsmd.getColumnCount();
        boolean rval = true;

        System.out.println();
        if (show_labels && rs.next()) {
            List names  = new ArrayList();
            for (int i = 1; i <= columns; i++) {
                names.add(rsmd.getColumnLabel(i));
            }
            System.out.println(names);
        }

        rs = stmt.executeQuery(SQL);
        while (rs.next()) {
            List values = new ArrayList();
            for (int i = 1; i <= columns; i++) {
                values.add(rs.getObject(i));
            }
            System.out.println(values);
            rval = false;
        }

        return rval;
    }

}
