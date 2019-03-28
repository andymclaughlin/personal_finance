package com.abm.pers_finance;
import java.io.File;
import java.util.Scanner;

/**
* This class handles ETL of the bank credit and debit transactions.
*/
public final class BankFileEtlManager {

/**
*This is a private contructor to enforce class as Singleton.
*/
    private BankFileEtlManger() { }

/**
*This is the main function to trigger the  bank ETL
*during developement. Will be replaced later.
*@param args the command line args, which are unused.
*/
    public static void main(final String[] args) {
        readFiles();

    }

/**
*This is a public utililty function to read the bank files into the database.
*/
    public static void readFiles() {
        String inputPath = "../../../input";
        String firstColumnHeader;
        Boolean isDebit;

        File dir = new File(inputPath);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
        for (File child : directoryListing) {
            try (Scanner in = new Scanner(child)) {
                firstColumnHeader = in.nextLine().split(",")[0];
                    if (firstColumnHeader == "Details") {
                        importDebitFile(child, in);
                    }
                } catch (IOException e) {
                    e.printStackTrace();

                }
        }
        }

    }
/**
*This is a helper function to import the debit file.
*@param f the file pointer to csv file for import.
*The cursor should be after the csv header.
*@param in the active Scanner pointer.
*/
    private static void importDebitFile(final File f, final Scanner in) {
        String header;
        String details;
        String postDate;
        String description;
        double amount;
        String type;
        double balance;
        String checkOrSlip;
        String checkSlip;
        String l;
        while ((l = in.nextLine()) != null) {
            String[] line = l.split(",");
            details = line[0];
            postDate = line[1];
            description = line[2];
            amount = Double.parseDouble(line[3]);
            type = line[4];
            balance = Double.parseDouble(line[5]);
            checkOrSlip = line[6];
            checkSlip = line[7];
        }
    }
}
