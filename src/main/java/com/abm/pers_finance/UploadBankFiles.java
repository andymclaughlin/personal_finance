package com.abm;
import java.io.File;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class UploadBankFiles{

	public static void main(String[] args) {
		readFiles();

	}
	public static void readFiles(){
		String inputPath = "../../../input";
		String firstColumnHeader;
		Boolean isDebit;

		File dir = new File(inputPath);
		  File[] directoryListing = dir.listFiles();
		  if (directoryListing != null) {
			for (File child : directoryListing) {
				try(Scanner in = new Scanner(child)){
					firstColumnHeader= in.nextLine().split(",")[0];
					if (firstColumnHeader=="Details"){
						importDebitFile(child, in);
					} else {
					}
				}
			
				catch (IOException e) {
            	e.printStackTrace();		
			  

			}
		  }
		}

	}
	public static void importDebitFile(File f, Scanner in){
		String header;
		String details;
		String postDate;
		String description;
		double amount;
		String type;
		double balance;
		String check_or_slip;
		String l;
		while((l= in.nextLine())!=null){
			String[] line= l.split(",");
			details= line[0];
			postDate = line[1];
			description = line[2];
			amount = Double.parseDouble(line[3]);
			type = line[4];
			balance = Double.parseDouble(line[5]);
			check_or_slip = line[6];
			
		
			


		}
	}
}
