/**
 * Distributed Databases - Assignment 03
 * HS-Fulda SoSe '17
 *
 * @author  Manasés Jesús
 */

import java.sql.*;
import java.util.*;

public class Assignment03 {

    private static final String URL     = "jdbc:oracle:thin:@__server__.hs-fulda.de:1521:__db-name__";
    private static final String USER    = "__REPLACE__";
    private static final String PASSWD  = "__REPLACE__";

    public static void main (String[] args) {
        new Assignment03().performTasks();
    }

    /** Perform all DB tasks
     * */
    protected void performTasks () {
        Connection connection = null;
        Statement stmt = null;
        ResultSet rs = null;
        long startTime = 0;

        try {
            connection = DriverManager.getConnection(URL, USER, PASSWD);
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            stmt = connection.createStatement();

            // Ready, set, go!
            for (String table : Arrays.asList("PROJA14.R1K", "PROJA14.R10K", "PROJA14.R100K", "PROJA14.R1000K")) {
                System.out.println("~~~~~~~ " + table + " ~~~~~~~");

                /***************************************************************************************/
                System.out.println("1. Full database mode");
                System.out.print("COUNT DISTINCT FIBO:    ");
                startTime = System.currentTimeMillis();
                rs = stmt.executeQuery("SELECT COUNT (DISTINCT FIBO) FROM " + table);
                rs.next();
                System.out.println(rs.getInt(1) + getElapsedTime(startTime, System.currentTimeMillis()));

                System.out.print("R1.PK = 2 * R2.PK:      ");
                startTime = System.currentTimeMillis();
                rs = stmt.executeQuery("SELECT COUNT (*) FROM " + table + " R1, " + table + " R2 WHERE R1.PK = 2 * R2.PK");
                rs.next();
                System.out.println(rs.getInt(1) + getElapsedTime(startTime, System.currentTimeMillis()));

                /***************************************************************************************/
                System.out.println("2. No database mode");
                System.out.print("COUNT DISTINCT FIBO:    ");
                Set<Long> fibo = new HashSet<>();

                // Getting results based on a cursor
                stmt.setFetchSize(5000);
                startTime = System.currentTimeMillis();
                rs = stmt.executeQuery("SELECT * FROM " + table);

                while (rs.next()) {
                    fibo.add(rs.getLong("FIBO"));
                }
                System.out.println(fibo.size() + getElapsedTime(startTime, System.currentTimeMillis()));

                System.out.print("R1.PK = 2 * R2.PK:      ");
                Set<Integer> pks = new HashSet<>();
                startTime = System.currentTimeMillis();
                rs = stmt.executeQuery("SELECT * FROM " + table);

                while (rs.next()) {
                    pks.add(rs.getInt("PK"));
                }
                int total = 0;
                for (int pk : pks) {
                    total += pks.contains(pk * 2) ? 1 : 0;
                }
                System.out.println(total + getElapsedTime(startTime, System.currentTimeMillis()));

                /***************************************************************************************/
                System.out.println("3. Partial database mode");
                System.out.print("COUNT DISTINCT FIBO:    ");
                fibo = new HashSet<>();

                startTime = System.currentTimeMillis();
                rs = stmt.executeQuery("SELECT FIBO FROM " + table);

                while (rs.next()) {
                    fibo.add(rs.getLong(1));
                }
                System.out.println(fibo.size() + getElapsedTime(startTime, System.currentTimeMillis()));

                System.out.print("R1.PK = 2 * R2.PK:      ");
                pks = new HashSet<>();
                startTime = System.currentTimeMillis();
                rs = stmt.executeQuery("SELECT PK FROM " + table);

                while (rs.next()) {
                    pks.add(rs.getInt(1));
                }
                total = 0;
                for (int pk : pks) {
                    total += pks.contains(pk * 2) ? 1 : 0;
                }
                System.out.println(total + getElapsedTime(startTime, System.currentTimeMillis()));

                // Turning off the cursor
                stmt.setFetchSize(0);
            }
        } catch (SQLException ex) {
            System.out.println("Rolling back data...");
            ex.printStackTrace();

            try {
                if (connection != null)
                    connection.rollback();
            } catch (SQLException sqle) {
                sqle.printStackTrace();
            }
        }
        finally {
            try {
                if (connection != null)
                    connection.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    /** Calculates the elapsed time
     *
     * @param start time (milliseconds)
     * @param end time (milliseconds)
     * @return a formatted string with the elapsed time
     */
    private String getElapsedTime (long start, long end) {
        long total = end - start;

        return "\nElapsed time:   \t\t" +
                (total >= 60000 ? (total / 1000 / 60) + "m, " + (total / 1000 % 60) + "s" : "") +
                (total >= 1000 && total < 60000 ? (total / 1000) + "s, " + (total % 1000) + "ms" : "") +
                (total < 1000 ? total + "ms" : "") + "\n";
    }
}
