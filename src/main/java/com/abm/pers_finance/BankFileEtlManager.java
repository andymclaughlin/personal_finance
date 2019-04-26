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
     * This is a private constructor to enforce class as Singleton.
     */
    private void BankFileEtlManger() {
    }

    /**
     * This is a public utility function to read the bank
     * files into the database.
     * @param username The database username.
     * @param password The database password.
    */
    static void readFiles(final String username, final String password) {
        String inputPath = System.getProperty("user.dir") + "/bank_files";
        String firstColumnHeader;


        File dir = new File(inputPath);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File child : directoryListing) {
                try (Scanner in = new Scanner(child)) {
                    firstColumnHeader = in.nextLine().split(",")[0];
                    if (firstColumnHeader.equals("Details")) {
                        importDebitFile(child, in, username, password);
                    } else {
                        importCreditFile(in, username, password);
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
     * @param username the database username.
     * @param password the database password.
    */
    private static void importDebitFile(final File f, final Scanner in,
         final String username, final String password) {

        String details;
        String postDate;
        String description;
        double amount;
        String type;
        double balance;
        int i;
        boolean hasNext;
        String l;

        Connection conn;
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
            i = 0;
            details = line[i];
            postDate = line[i++];
            description = line[i++];
            amount = Double.parseDouble(line[i++]);
            type = line[i++];
            balance = Double.parseDouble(line[i]);
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
            Date parsed = null;
            try {
                parsed = format.parse(postDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            java.sql.Date postDateSql = new java.sql.Date(parsed.getTime());

            String sql = "INSERT INTO public.debit_trans"
                    + "(details, posting_date, description, amount,"
                    + " type, balance) VALUES"
                    + "(?,?,?,?, ?, ?)";

            try {
                i = 1;
                ps = conn.prepareStatement(sql);
                ps.setString(i++, details);

                ps.setDate(i++, postDateSql);
                ps.setString(i++, description);
                ps.setDouble(i++, amount);
                ps.setString(i++, type);
                ps.setDouble(i, balance);


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
            hasNext = in.hasNext();
            if (!hasNext) {
                break;
            }
        }

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }


    /**
     * This is a helper function to import the credit card file.
     *
     * @param in the active Scanner pointer.
     * @param username the database username.
     * @param password the database password.
     */
    private static void importCreditFile(final Scanner in,
              final String username, final String password) {
        String tranDate;
        String postDate;
        String description;
        String category;
        String type;
        double amount;
        String l;
        Connection conn;
        conn = DBConnectionManager.getDBConnection(username, password);
        int i;

        PreparedStatement ps = null;

        while ((l = in.nextLine()) != null) {
            String[] line = l.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            i = 0;
            tranDate = line[i++];
            postDate = line[i++];
            description = line[i++];
            category = line[i++];
            type = line[i++];
            amount = Double.parseDouble(line[i]);
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
            Date parsedTranDate = null;
            Date parsedPostDate = null;
            try {
                parsedTranDate = format.parse(tranDate);
                parsedPostDate = format.parse(postDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            java.sql.Date tranDateSql = new java.sql.Date(
                parsedTranDate.getTime());


            java.sql.Date postDateSql = new java.sql.Date(
                parsedPostDate.getTime());

            String sql = "INSERT INTO public.credit_trans"
                    + "(tran_date, post_date, description,"
                    + " category, tran_type, amount) VALUES"
                    + "(?,?,?,?, ?, ?)";

            try {

                ps = conn.prepareStatement(sql);
                i = 1;
                ps.setDate(i++, tranDateSql);
                ps.setDate(i++, postDateSql);
                ps.setString(i++, description);
                ps.setString(i++, category);
                ps.setString(i++, type);
                ps.setDouble(i, amount);


                ps.executeUpdate();


            } catch (SQLException e) {

                System.out.println(e.getMessage());

            } finally {

                try {
                    if (ps != null) {
                        ps.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
            if (!in.hasNext()) {
                break;
            }
        }
    }
}
