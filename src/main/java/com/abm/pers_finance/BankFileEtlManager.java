package com.abm.pers_finance;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

/**
* This class handles ETL of the bank credit and debit transactions.
*/
public final class BankFileEtlManager {

    /**
     * This is a private contructor to enforce class as Singleton.
     */
    private void BankFileEtlManger() {
    }

    /**
     * This is a public utililty function to read the bank files into the database.
     */
    public static void readFiles(String username, String password) {
        String inputPath = System.getProperty("user.dir") + "/bank_files";
        String firstColumnHeader;
        Boolean isDebit;

        File dir = new File(inputPath);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File child : directoryListing) {
                try (Scanner in = new Scanner(child)) {
                    firstColumnHeader = in.nextLine().split(",")[0];
                    if (firstColumnHeader.equals("Details")) {
                        importDebitFile(child, in, username, password);
                    } else{
                        importCreditFile(child, in, username, password);
                    }
                } catch (IOException e) {
                    e.printStackTrace();

                }
            }
        }

    }

    /**
     * This is a helper function to import the debit card file.
     *
     * @param f  the file pointer to csv file for import.
     *           The cursor should be after the csv header.
     * @param in the active Scanner pointer.
     */
    private static void importDebitFile(final File f, final Scanner in,
                                        String username, String password) {

        String details;
        String postDate;
        String description;
        double amount;
        String type;
        double balance;

        String l;

        Connection conn = null;
        PreparedStatement ps = null;

        String clearTableSql = "delete from public.debit_trans";

        conn = DBConnectionManager.getDBConnection(username, password);
        try {
            ps = conn.prepareStatement(clearTableSql);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        while ((l = in.nextLine()) != null) {
            String[] line = l.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            details = line[0];
            postDate = line[1];
            description = line[2];
            amount = Double.parseDouble(line[3]);
            type = line[4];
            balance = Double.parseDouble(line[5]);
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
            Date parsed = null;
            try {
                parsed = format.parse(postDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            java.sql.Date postDateSql = new java.sql.Date(parsed.getTime());

            String sql = "INSERT INTO public.debit_trans"
                    + "(details, posting_date, description, amount, type, balance) VALUES"
                    + "(?,?,?,?, ?, ?)";

            try {

                ps = conn.prepareStatement(sql);
                ps.setString(1, details);

                ps.setDate(2, postDateSql);
                ps.setString(3, description);
                ps.setDouble(4, amount);
                ps.setString(5, type);
                ps.setDouble(6, balance);


                ps.executeUpdate();


            } catch (SQLException e) {

                System.out.println(e.getMessage());

            } finally {

                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }


            }
            if (in.hasNext() == false) {
                break;
            }
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
     * This is a helper function to import the credit card file.
     *
     * @param f  the file pointer to csv file for import.
     *           The cursor should be after the csv header.
     * @param in the active Scanner pointer.
     */
    private static void importCreditFile(final File f, final Scanner in, final String username, final String password) {
        String tranDate;
        String postDate;
        String description;
        String category;
        String type;
        double amount;
        String l;
        Connection conn = null;
        conn = DBConnectionManager.getDBConnection(username, password);

        PreparedStatement ps = null;

        while ((l = in.nextLine()) != null) {
            String[] line = l.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            tranDate = line[0];
            postDate = line[1];
            description = line[2];
            category = line[3];
            type = line[4];
            amount = Double.parseDouble(line[5]);
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
            Date parsedTranDate = null;
            Date parsedPostDate = null;
            try {
                parsedTranDate = format.parse(tranDate);
                parsedPostDate = format.parse(postDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            java.sql.Date tranDateSql = new java.sql.Date(parsedTranDate.getTime());
            java.sql.Date postDateSql = new java.sql.Date(parsedPostDate.getTime());

            String sql = "INSERT INTO public.credit_trans"
                    + "(tran_date, post_date, description, category, tran_type, amount) VALUES"
                    + "(?,?,?,?, ?, ?)";

            try {

                ps = conn.prepareStatement(sql);
                ps.setDate(1, tranDateSql);
                ps.setDate(2, postDateSql);
                ps.setString(3, description);
                ps.setString(4, category);
                ps.setString(5, type);
                ps.setDouble(6, amount);


                ps.executeUpdate();


            } catch (SQLException e) {

                System.out.println(e.getMessage());

            } finally {

                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }


            }
            if (in.hasNext() == false) {
                break;
            }
        }
    }
}
