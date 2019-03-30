package com.abm.pers_finance;

import org.apache.commons.cli.*;


public class CLIParser {
    public static void main(String[] args){
        Options  options = new Options();
        options.addOption("i", false, "import bank files from input folder.");
        options.addOption("n", false, "print largest unlabelled bank transaction description.");
        options.addOption("u", true, "username");
        options.addOption("p", true, "password");



        CommandLineParser parser = new DefaultParser();


        try {
            CommandLine cmd = parser.parse( options, args);
            String username = cmd.getOptionValue("u");
            String password = cmd.getOptionValue("p");
            if(cmd.hasOption("i")) {
                BankFileEtlManager.readFiles(username, password);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
