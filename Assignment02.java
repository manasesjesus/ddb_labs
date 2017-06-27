/**
 * Distributed Databases - Assignment 02
 * HS-Fulda SoSe 17
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


public class Assignment02 {
    /* Database objects and credentials */
    static final String URL        = "jdbc:oracle:thin:@__server__.hs-fulda.de:1521:__db-name__";
    static final String USER       = "__REPLACE__";
    static final String PASSWD     = "__REPLACE__";

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

        System.out.println("Establishing DB connection...");
        try {
            // Establish connection
            connection = DriverManager.getConnection(URL, USER, PASSWD);
            connection.setAutoCommit(false);
            //connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            // Prepare statement
            stmt = connection.createStatement();
            
            /* Insert 1,000 10,000 and 100,000 tuples and name the tables R1K, R10K, R100K respectively */
            Map<String, Integer> tables = new HashMap<>();

            tables.put("R1K", 1000);
            tables.put("R10K", 10000);
            tables.put("R100K", 100000);

            for (String table : tables.keySet()) {
                createTable(table);
                insertTuples(table, tables.get(table));

                /* Count and count unique for all columns,
                 * Maximum and minimum values of all numerical columns,
                 * Number of columns contained in the table
                 */
                rs = stmt.executeQuery("SELECT * FROM " + table);
                System.out.println("COLUMNS(" + rs.getMetaData().getColumnCount() + ") \t ROWS  UNIQUE  \t\t\t MAX \t MIN");

                SQL = "SELECT COUNT(col), COUNT(DISTINCT col), MAX(col), MIN(col) FROM " + table;

                for (String column : Arrays.asList("PK", "CK1", "FK", "FIBO", "GV100", "GV10000", "UV30", "LV1000")) {
                    rs = stmt.executeQuery(SQL.replaceAll("col", column));
                    rs.next();
                    System.out.format("%-10s \t %8d %7d %14d %7d \n", column, rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getInt(4));
                }

                SQL = "SELECT COUNT(col), COUNT(DISTINCT col) FROM " + table;

                for (String column : Arrays.asList("STADT100", "DAT100")) {
                    rs = stmt.executeQuery(SQL.replaceAll("col", column));
                    rs.next();
                    System.out.format("%-10s \t %8d %7d \n", column, rs.getInt(1), rs.getInt(2));
                }
            }

        } catch (SQLException | IOException ex) {
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

    /** Load a SQL file and create a table with the specified name
     * */
    private void createTable (String name) throws SQLException, IOException {
        List<String> lines = Files.readAllLines(Paths.get("./src/assignment02.sql"), Charset.defaultCharset());
        String all = "";

        for (String line : lines) {
            all += line;
        }

        String SQLs[] = all.replaceAll("R1K", name).split(";");
        stmt.executeUpdate(SQLs[1]);        // TODO: Drop table if exist
        stmt.executeUpdate(SQLs[2]);        // Create table

        System.out.print("\nTable " + name + " created. ");
    }

    /* Generate and insert into de DB the given number of tuples
     */
    private void insertTuples (String table, int total) throws SQLException {
        System.out.print("Generating and inserting tuples.");

        String sql = "INSERT INTO " + table + "(CK1, FK, FIBO, GV100, GV10000, UV30, LV1000, STADT100, DAT100) " +
                     "VALUES (?,?,?,?,?,?,?,?,?)";
        final String[] cities = {"Fulda", "Frankfurt", "Cologne", "Guadalajara", "Oaxaca", "London", "Manchester",
                                 "Utrecht", "Amsterdam", "Paris", "Milan", "Moscow", "Krakow", "Wroclaw",
                                 "Lviv", "Kiev", "Zurich", "Skagen", "Bucharest", "Helsinki"};
        Random rand     = new Random();
        long daymilis   = 1000 * 60 * 60 * 24;
        int r1000       = 0;
        PreparedStatement pstmt = connection.prepareStatement(sql);

        long startTime  = System.currentTimeMillis();

        for (int pk=1, fibo=1; pk<=total; pk++, fibo++) {
            // PK(auto), CK1
            if (pk %  5 == 0) {
                pstmt.setNull(1,Types.BIGINT);
            }
            else {
                pstmt.setInt(1, pk * 3);
            }

            // FK
            if (pk % 10 == 0) {
                pstmt.setNull(2,Types.BIGINT);
            }
            else {
                pstmt.setInt(2, pk % 10);
            }

            //  FIBO   < Integer.MAX_VALUE
            pstmt.setInt(3, iFibonacci(fibo));
            if (fibo >= 46) {
                fibo = 0;
            }

            // GV100   [1,100]
            pstmt.setInt(4, rand.nextInt(100) + 1);

            // GV10000 [1,10000]
            pstmt.setInt(5, rand.nextInt(10000) + 1);

            // Random  [1,1000]  =>  UV30
            r1000 = rand.nextInt(1000) + 1;
            if (1 <= r1000 && r1000 <= 899) {
                pstmt.setInt(6, r1000/100 + 1);
            }
            else if (900 <= r1000 && r1000 <= 989) {
                pstmt.setInt(6, (r1000/10)%10 + 11);
            }
            else {
                pstmt.setInt(6, r1000%10 + 20);
            }

            // LV1000
            if (r1000%2 == 0 || r1000%3 == 0 || r1000%5 == 0 || r1000%7 == 0) {
                pstmt.setNull(7,Types.BIGINT);
            }
            else {
                pstmt.setInt(7, r1000);
            }

            // STADT100
            pstmt.setString(8, cities[rand.nextInt(cities.length)]);

            // DAT100
            pstmt.setDate(9, new java.sql.Date(startTime + daymilis * rand.nextInt(100)));
            pstmt.addBatch();

            // Doing batch size of 1000 to balance between network iteration and avoid out of memory error
            if (pk % 1000 == 0) {
                pstmt.executeBatch();
                pstmt.clearBatch();
                System.out.print(".");
            }
        }

        System.out.println("\nIt took " + (System.currentTimeMillis() - startTime) + "ms to insert " + total + " tuples");
    }

    /** Fibonacci method  (iterative)
     * */
    private int iFibonacci (int n) {
        int x = 0;

        for (int i=0, y=1, z=1; i<n; i++) {
            x = y;
            y = z;
            z = x + y;
        }
        return x;
    }
}


