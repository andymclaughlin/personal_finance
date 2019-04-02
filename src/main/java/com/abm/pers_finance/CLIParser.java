package com.abm.pers_finance;

import org.apache.commons.cli.*;

import java.util.Scanner;


public class CLIParser {
    public static void main(String[] args){

        Options  options = new Options();
        options.addOption("i", false, "import bank files from input folder.");
        options.addOption("n", false, "print largest unlabelled bank transaction description.");
        options.addOption("u", true, "username");
        options.addOption("p", true, "password");
        options.addOption("c", false, "clear mapping tables");
        options.addOption("f", true, "write financials of specified year to folder");
        options.addOption("x", true, "used to unmap description");
        options.addOption("d", true, "delete specified account code");




        CommandLineParser parser = new DefaultParser();

        Scanner scanner = new Scanner(System.in);

        try {
            CommandLine cmd = parser.parse( options, args);
            String username = cmd.getOptionValue("u");
            String password = cmd.getOptionValue("p");
            if(cmd.hasOption("c")){
                CliQueryManager.clearMappingTables(username, password);
            }
            if(cmd.hasOption("i")) {
                BankFileEtlManager.readFiles(username, password);
            }
            if(cmd.hasOption("f")) {
                int fy = Integer.parseInt(cmd.getOptionValue("f"));
                CliQueryManager.writeFinancialStatementToCsv(username, password, fy);
                CliQueryManager.writeTransWithLabelsToCsv(username, password);
            }
            if(cmd.hasOption("x")) {
                CliQueryManager.unMapDescription(username, password, cmd.getOptionValue("x"));
            }
            if(cmd.hasOption("d")) {
                CliQueryManager.deleteAccountCode(username, password, cmd.getOptionValue("d"));
            }
            if(cmd.hasOption("n")){
                while(true) {
                    String tranDescrip=CliQueryManager.getNextUnlabelledDescrip(username, password);
                    System.out.println("Enter the account code for this transaction. "+
                        "Type end to exit.");

                    String acctCode = scanner.nextLine();
                    if (acctCode.equals("end")){
                        break;
                    }
                    boolean acctCodeExists = CliQueryManager.doesAccCodeExist(username, password, acctCode);

                    if (acctCodeExists == false) {
                        System.out.println("Enter a description for the new account code:");
                        String acctCodeDescription = scanner.nextLine();
                        CliQueryManager.createAcctCode(username, password, acctCode, acctCodeDescription);
                    }
                    CliQueryManager.mapDescription(username, password, tranDescrip, acctCode);
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
