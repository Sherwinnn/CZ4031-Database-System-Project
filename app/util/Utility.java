package app.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import app.Constants;
import app.storage.Record;
import app.util.Analyzer;


public class Utility {

	/**
	 * Loading records from the data file
	 * @param filePath to data file
	 * @return array of records
	 * @throws Exception
	 */
	public static List<Record> readRecord(String filePath) throws Exception {
		File dataFile = new File(filePath);
		System.out.println("----------------------------------Information of Data-------------------------------------------------");
		System.out.println("Loading records from "+filePath);
		
		if (!dataFile.exists()) {
			dataFile = new File(Constants.PROJECT_DIRECTORY, filePath);
			System.out.println("Reading Records from " + dataFile.getAbsolutePath());
			if (!dataFile.exists()){
				throw new FileNotFoundException("File doesnt exist, check file path!");
			}
		}
		
		// initialising a new arraylist of records
		List<Record> records = new ArrayList<>();
		String line;
		String[] parts = null;
		BufferedReader TSVReader = null;
		try {
			TSVReader = new BufferedReader( new FileReader(dataFile));
			// reading header first (to be skipped)
			TSVReader.readLine();
			while((line = TSVReader.readLine()) != null) {
				parts = line.split("\\t");
				Record record = new Record(Float.parseFloat(parts[1]), Integer.parseInt(parts[2]), parts[0]);
				records.add( record );
				Analyzer.analysis(record);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (TSVReader != null) {
				try {
					TSVReader.close();
				}catch (IOException e) {
					System.out.println(e.getMessage());
				}
			}
		}
		System.out.println("Total Records: "+ records.size());
		Analyzer.overallinfo();
		return records;
	}

	/**
	 * Convert file size into human-readable format
	 * @param size file size in bytes
	 * @return formatted size
	 */
	public static String formatFileSize(int size){
		String[] suffix = { "B", "KB", "MB", "GB", "TB" };
		int order = 0;
		if (size>0){
			order = (int) (Math.log(size)/Math.log(1000));
		}
		double normSize = size / Math.pow(1000, order);
		return String.format("%.2f %s", normSize, suffix[order]);
	}


	public static List<Record> generateRecords(int num){
		ArrayList<Record> records = new ArrayList<>();
		for (int i = 0; i < num; i++) {
			String tconst = String.format("tt%08d", i+1);
			records.add( new Record(0f, i+1, tconst));
		}
		return records;
	}

	public static List<Record> generateRecords(int num, int duplicates){
		ArrayList<Record> records = new ArrayList<>();
		for (int i = 0; i < num; i++) {
			String tconst = String.format("tt%08d", i+1);
			records.add( new Record(0f, i/duplicates, tconst));
		}
		return records;
	}
}