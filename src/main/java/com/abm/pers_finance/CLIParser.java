package com.abm.pers_finance;



import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Scanner;

/**
 * This class handles parsing of the command line interface.
 */
final class CLIParser {

    /**
     * This is a private constructor to enforce class as Singleton.
     */
    private void CLIParser() {
    }

    /**
     * This is the main class method run for the CLI.
     * @param args The CLI args.
     * @throws ParseException thrown if args do not conform.
     */
    public static void main(final String[] args) throws ParseException {

        Options options = new Options();
        options.addOption("i", false, "import bank files from input folder.");
        options.addOption("n", false, "print largest unlabelled bank "
                + "transaction description.");
        options.addOption("u", true, "username");
        options.addOption("p", true, "password");
        options.addOption("c", false, "clear mapping tables");
        options.addOption("f", true, "write financials of specified year "
                + "to folder");
        options.addOption("x", true, "used to unmap description");
        options.addOption("d", true, "delete specified account code");




        CommandLineParser parser = new DefaultParser();

        Scanner scanner = new Scanner(System.in);

        try {
            CommandLine cmd = parser.parse(options, args);
            String username = cmd.getOptionValue("u");
            String password = cmd.getOptionValue("p");
            if (cmd.hasOption("c")) {
                CliQueryManager.clearMappingTables(username, password);
            }
            if (cmd.hasOption("i")) {
                BankFileEtlManager.readFiles(username, password);
            }
            if (cmd.hasOption("f")) {
                int fy = Integer.parseInt(cmd.getOptionValue("f"));
                CliQueryManager.writeFinancialStatementToCsv(username,
                        password, fy);
                CliQueryManager.writeTransWithLabelsToCsv(username, password);
            }
            if (cmd.hasOption("x")) {
                CliQueryManager.unMapDescription(username, password,
                        cmd.getOptionValue("x"));
            }
            if (cmd.hasOption("d")) {
                CliQueryManager.deleteAccountCode(username, password,
                        cmd.getOptionValue("d"));
            }
            if (cmd.hasOption("n")) {
                while (true) {
                    String tranDes = CliQueryManager.getNextUnlabelledDescrip(
                            username, password);
                    System.out.println("Enter the account code for this "
                            + "transaction. Type end to exit.");

                    String acctCode = scanner.nextLine();
                    if (acctCode.equals("end")) {
                        break;
                    }
                    boolean acctCodeExists = CliQueryManager.doesAccCodeExist(
                            username, password, acctCode);

                    if (!acctCodeExists) {
                        System.out.println("Enter a description for the new "
                                + "account code:");
                        String acctCodeDescription = scanner.nextLine();
                        CliQueryManager.createAcctCode(username, password,
                                acctCode, acctCodeDescription);
                    }
                    CliQueryManager.mapDescription(username, password, tranDes,
                            acctCode);
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
