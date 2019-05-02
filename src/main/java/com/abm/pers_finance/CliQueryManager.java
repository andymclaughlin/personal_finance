package com.abm.pers_finance;
import com.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * This class handles read write query operations for the CLI.
 */
public final class CliQueryManager {

    /**
     * This is a private contructor to enforce class as Singleton.
     */
    private void CliQueryManager() {
    }

    /**
     * This is a helper function to print the next unlabelled description.
     *
     * @param username  the database username
     * @param password the database password.
     * @return returns the next unlabelled bank transaction description
     */
    public static String getNextUnlabelledDescrip(final String username,
                                                  final String password) {


        Connection conn = null;
        PreparedStatement ps = null;
        String tranDescrip = "";
        String nextSql = "select description, amount, tran_count"
                + " closest_mapped_descrip, similarity, acct_code"
                + " from public.top_unmapped_description";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(nextSql);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();

            int columnsNumber = rsmd.getColumnCount();
            System.out.println("Next umapped description.");
            while (rs.next()) {
                tranDescrip = rs.getString(1);
                for (int i = 1; i <= columnsNumber; i++) {

                    String columnValue = rs.getString(i);
                    System.out.println(rsmd.getColumnName(i) + ":");
                    System.out.println(columnValue + "\n");
                }
                System.out.println("");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return tranDescrip;

    }

    /**
     * This is a helper function that returns true if given acct code exists,
     * false otherwise.
     *
     * @param username  the database username
     * @param password the database password.
     * @param acctCode the account code string
     * @return returns whether or not the account code exists
     */
    public static boolean doesAccCodeExist(final String username,
           final String password, final String acctCode) {

        boolean returnValue = false;
        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "select count(*) as row_count from "
                + "acct_code_to_acct_description "
                + "where acct_code=?";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, acctCode);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();

            int columnsNumber = rsmd.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {

                    int rowCount = rs.getInt(i);
                    if (rowCount == 0) {
                        returnValue = false;
                    } else {
                        returnValue = true;
                    }
                }
                System.out.println("");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return returnValue;
    }

    /**
     * This functions write the specified financial statement to csv.
     *
     * @param username  the database username
     * @param password the database password.
     * @param fy the year the  financial statements are run as of
     */
    public static void writeFinancialStatementToCsv(final String username,
            final String password, final int fy) {


        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "select acct_descrip, "
                + "sum(case when extract(month from post_date)= "
                + "1 then amount else 0.0 end) as p1, "
                + "sum(case when extract(month from post_date)= "
                + "2 then amount else 0.0 end) as p2, "
                + "sum(case when extract(month from post_date)= "
                + " 3 then amount else 0.0 end) as p3, "
                + "sum(case when extract(month from post_date)= "
                + "4 then amount else 0.0 end) as p4, "
                + "sum(case when extract(month from post_date)= "
                + "5 then amount else 0.0 end) as p5, "
                + "sum(case when extract(month from post_date)= "
                + "6 then amount else 0.0 end) as p6, "
                + "sum(case when extract(month from post_date)= "
                + "7 then amount else 0.0 end) as p7, "
                + "sum(case when extract(month from post_date)= "
                + "8 then amount else 0.0 end) as p8, "
                + "sum(case when extract(month from post_date)= "
                + "9 then amount else 0.0 end) as p9, "
                + "sum(case when extract(month from post_date)= "
                + "10 then amount else 0.0 end) as p10, "
                + "sum(case when extract(month from post_date)= "
                + "11 then amount else 0.0 end) as p11, "
                + "sum(case when extract(month from post_date)= "
                + "12 then amount else 0.0 end) as p12, "
                + "sum(amount) as total "
                + "from "
                + "merged_trans, descrip_to_acct_code, "
                + "acct_code_to_acct_description "
                + "where merged_trans.description = "
                + "descrip_to_acct_code.description and "
                + "descrip_to_acct_code.acct_code = "
                + "acct_code_to_acct_description.acct_code and "
                + "extract(year from post_date)=? "
                + "group by "
                + "acct_descrip "
                + "order by abs(sum(amount)) desc ";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);
            ps.setInt(1, fy);
            ResultSet rs = ps.executeQuery();


            String fileUrl = System.getProperty("user.dir")
                    + "/output/fs_" + fy + ".csv";

            File file = new File(fileUrl);


            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            FileWriter fw = null;
            try {
                fw = new FileWriter(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
            BufferedWriter writer = new BufferedWriter(fw);
            try {

                CSVWriter csvWriter = new CSVWriter(writer,
                        CSVWriter.DEFAULT_SEPARATOR,
                        CSVWriter.NO_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END);
                Boolean includeHeaders = true;



                try {
                    csvWriter.writeAll(rs, includeHeaders);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }



        } catch (SQLException e) {
            e.printStackTrace();
        }


        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }



    /**
     * This functions writes the tran label mapping to csv.
     *
     * @param username  the database username
     * @param password the database password.
     */

    public static void writeTransWithLabelsToCsv(
            final String username, final String password) {


        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "select * "
                + "from "
                + "merged_trans, descrip_to_acct_code, "
                + "acct_code_to_acct_description "
                + "where merged_trans.description = "
                + "descrip_to_acct_code.description and "
                + "descrip_to_acct_code.acct_code = "
                + "acct_code_to_acct_description.acct_code ";


        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);

            ResultSet rs = ps.executeQuery();


            String fileUrl = System.getProperty("user.dir")
                    + "/output/bank_trans_with_labels.csv";

            File file = new File(fileUrl);


            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            FileWriter fw = null;
            try {
                fw = new FileWriter(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
            BufferedWriter writer = new BufferedWriter(fw);
            try {

                CSVWriter csvWriter = new CSVWriter(writer,
                        CSVWriter.DEFAULT_SEPARATOR,
                        CSVWriter.NO_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END);
                Boolean includeHeaders = true;



                try {
                    csvWriter.writeAll(rs, includeHeaders);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }



        } catch (SQLException e) {
            e.printStackTrace();
        }


        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * This is a helper function that returns true if given tran
     * description exists,
     * false otherwise.
     *
     * @param username  the database username
     * @param password the database password.
     * @param tranDescrip the bank transaction description
     * @return whether or not tran description exists.
     */
    public static boolean doesTranDescripExist(final String username,
           final String password, final String tranDescrip) {

        boolean returnValue = false;
        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "select count(*) as row_count from "
                + "descrip_to_acct_code "
                + "where description=?";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, tranDescrip);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();

            int columnsNumber = rsmd.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {

                    int rowCount = rs.getInt(i);
                    if (rowCount == 0) {
                        returnValue = false;
                    } else {
                        returnValue = true;
                    }
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return returnValue;
    }

    /**
     * This is a helper function to create an acct code record.
     *
     * @param username  the database username
     * @param password the database password.
     * @param acctCode account code string
     * @param acctCodeDescription description for account code
     */
    public static void createAcctCode(final String username,
          final String password, final String acctCode,
           final String acctCodeDescription) {


        Connection conn;
        PreparedStatement ps;

        String sql = "insert into  "
                + "acct_code_to_acct_description "
                + "values (?, ?)";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, acctCode);
            ps.setString(2, acctCodeDescription);
            ps.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }


        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }


    /**
     * This is a helper function to map a bank description to
     * an account code.
     *
     * @param username  the database username
     * @param password the database password.
     * @param descrip the bank transaction description
     * @param acctCode the account code to map to
     */
    public static void mapDescription(
            final String username, final String password,
            final String descrip, final String acctCode) {

        if (!doesAccCodeExist(username, password, acctCode)) {
            return;
        }
        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "insert into  "
                + "descrip_to_acct_code "
                + "values (?, ?)";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, descrip);
            ps.setString(2, acctCode);
            ps.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }


        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }


    /**
     * This is a helper function to create an acct code record.
     *
     * @param username  the database username
     * @param password the database password.
     * @param descrip the bank description to unmap
     */
    public static void unMapDescription(final String username,
        final String password, final String descrip) {


        Connection conn;
        PreparedStatement ps;

        String sql = "delete from  "
                + "descrip_to_acct_code where trim(description)=?";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, descrip);
            ps.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }


        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }


    /**
     * This is a helper function to clear the mapping tables.
     *
     * @param username  the database username
     * @param password the database password.
     */

    public static void clearMappingTables(
            final String username, final String password) {
        Connection conn;
        conn = DBConnectionManager.getDBConnection(username, password);

        try {
            Statement s = conn.createStatement();

            String s1 = "delete from  "
                    + "acct_code_to_acct_description; ";
            String s2 = "delete from  "
                    + "descrip_to_acct_code; ";

            s.addBatch(s1);
            s.addBatch(s2);


            s.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * This is a helper function to delete an account code.
     *
     * @param username  the database username
     * @param password the database password.
     * @param oldAccountCode account code to delete
     */

    public static void deleteAccountCode(
            final String username, final String password,
            final String oldAccountCode) {
        Connection conn = null;
        conn = DBConnectionManager.getDBConnection(username, password);

        try {

            String s1 = "delete from "
                    + "acct_code_to_acct_description "
                    + "where acct_code= ? ";

            PreparedStatement s = conn.prepareStatement(s1);
            s.setString(1, oldAccountCode);
            s.executeUpdate();

            String sql2 = "delete from "
                    + "descrip_to_acct_code "
                    + "where acct_code= ? ";

            PreparedStatement s2 = conn.prepareStatement(sql2);
            s2.setString(1, oldAccountCode);

            s2.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }


}
