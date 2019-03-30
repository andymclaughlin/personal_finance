package com.abm.pers_finance;
import java.io.File;
import java.io.IOException;
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


}
