package com.abm.pers_finance;
import com.opencsv.CSVWriter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

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
     */
    public static String getNextUnlabelledDescrip(String username, String password) {


        Connection conn = null;
        PreparedStatement ps = null;
        String tranDescrip="";
        String nextSql = "select description, amount, tran_count"+
                " from public.top_unmapped_description";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(nextSql);
            ResultSet rs =ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();

            int columnsNumber = rsmd.getColumnCount();
            System.out.println("Next umapped description.");
            while (rs.next()) {
                tranDescrip= rs.getString(1);
                for (int i = 1; i <= columnsNumber; i++) {

                    String columnValue = rs.getString(i);
                    System.out.println(rsmd.getColumnName(i)+":");
                    System.out.println(columnValue+"\n");
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
     */
    static public boolean doesAccCodeExist(String username, String password, String acctCode) {

        boolean returnValue=false;
        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "select count(*) as row_count from "+
                "acct_code_to_acct_description "+
                "where acct_code=?";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, acctCode);
            ResultSet rs =ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();

            int columnsNumber = rsmd.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {

                    int rowCount= rs.getInt(i);
                    if (rowCount==0){
                        returnValue= false;
                    } else{
                        returnValue= true;
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
     * This is a helper function that returns true if given acct code exists,
     * false otherwise.
     *
     * @param username  the database username
     * @param password the database password.
     */
    static public void writeFinancialStatementToCsv(String username, String password, int fy) {


        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "select acct_descrip, " +
                "sum(case when extract(month from post_date)=1 then amount else 0.0 end) as p1, " +
                "sum(case when extract(month from post_date)=2 then amount else 0.0 end) as p2, " +
                "sum(case when extract(month from post_date)=3 then amount else 0.0 end) as p3, " +
                "sum(case when extract(month from post_date)=4 then amount else 0.0 end) as p4, " +
                "sum(case when extract(month from post_date)=5 then amount else 0.0 end) as p5, " +
                "sum(case when extract(month from post_date)=6 then amount else 0.0 end) as p6, " +
                "sum(case when extract(month from post_date)=7 then amount else 0.0 end) as p7, " +
                "sum(case when extract(month from post_date)=8 then amount else 0.0 end) as p8, " +
                "sum(case when extract(month from post_date)=9 then amount else 0.0 end) as p9, " +
                "sum(case when extract(month from post_date)=10 then amount else 0.0 end) as p10, " +
                "sum(case when extract(month from post_date)=11 then amount else 0.0 end) as p11, " +
                "sum(case when extract(month from post_date)=12 then amount else 0.0 end) as p12, " +
                "sum(amount) as total " +
                "from " +
                "merged_trans, descrip_to_acct_code, acct_code_to_acct_description " +
                "where merged_trans.description = descrip_to_acct_code.description and "+
                "descrip_to_acct_code.acct_code = acct_code_to_acct_description.acct_code and "+
                "extract(year from post_date)=? " +
                "group by " +
                "acct_descrip " +
                "order by abs(sum(amount)) desc ";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);
            ps.setInt(1, fy);
            ResultSet rs =ps.executeQuery();


            String fileUrl = System.getProperty("user.dir")  + "/output/fs_" + fy+".csv";

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
                        CSVWriter.DEFAULT_LINE_END);Boolean includeHeaders = true;



                try {
                    csvWriter.writeAll(rs, includeHeaders);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            };



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

    static public void writeTransWithLabelsToCsv(String username, String password) {


        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "select * " +
                "from " +
                "merged_trans, descrip_to_acct_code, acct_code_to_acct_description " +
                "where merged_trans.description = descrip_to_acct_code.description and "+
                "descrip_to_acct_code.acct_code = acct_code_to_acct_description.acct_code ";


        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);

            ResultSet rs =ps.executeQuery();


            String fileUrl = System.getProperty("user.dir")  + "/output/bank_trans_with_labels.csv";

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
                        CSVWriter.DEFAULT_LINE_END);Boolean includeHeaders = true;



                try {
                    csvWriter.writeAll(rs, includeHeaders);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            };



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
     * This is a helper function that returns true if given acct code exists,
     * false otherwise.
     *
     * @param username  the database username
     * @param password the database password.
     */
    static public boolean doesTranDescripExist(String username, String password, String tranDescrip) {

        boolean returnValue=false;
        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "select count(*) as row_count from "+
                "descrip_to_acct_code "+
                "where description=?";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, tranDescrip);
            ResultSet rs =ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();

            int columnsNumber = rsmd.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {

                    int rowCount= rs.getInt(i);
                    if (rowCount==0){
                        returnValue= false;
                    } else{
                        returnValue= true;
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
     */
    static public void createAcctCode(String username, String password, String acctCode,
                                  String acctCodeDescription) {

        boolean returnValue=false;
        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "insert into  "+
                "acct_code_to_acct_description "+
                "values (?, ?)";

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
     * This is a helper function to create an acct code record.
     *
     * @param username  the database username
     * @param password the database password.
     */
    static public void mapDescription(String username, String password, String descrip,
                                      String acctCode) {

        if (doesAccCodeExist(username, password, acctCode)==false){
            return;
        };
        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "insert into  "+
                "descrip_to_acct_code "+
                "values (?, ?)";

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
     */
    static public void unMapDescription(String username, String password, String descrip) {


        Connection conn = null;
        PreparedStatement ps = null;

        String sql = "delete from  "+
                "descrip_to_acct_code where trim(description)=?";

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
    public static void clearMappingTables(String username, String password){
        Connection conn = null;
        conn = DBConnectionManager.getDBConnection(username, password);

        try {
            Statement s = conn.createStatement();

            String s1 = "delete from  "+
                    "acct_code_to_acct_description; ";
            String s2 = "delete from  "+
                    "descrip_to_acct_code; ";

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

    public static void deleteAccountCode(String username, String password, String oldAccountCode){
        Connection conn = null;
        conn = DBConnectionManager.getDBConnection(username, password);

        try {

            String s1 = "delete from "+
                    "acct_code_to_acct_description " +
                    "where acct_code= ? ";

            PreparedStatement s = conn.prepareStatement(s1);
            s.setString(1, oldAccountCode);
            s.executeUpdate();

            String sql2 = "delete from "+
                    "descrip_to_acct_code "+
                    "where acct_code= ? ";

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
