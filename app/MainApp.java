
package app;
 
import java.util.*;

import app.storage.Disk;
import app.storage.Record;
import app.util.Utility;
import app.index.BPlusTree;
import app.storage.Address;

public class MainApp implements Constants {
	
	Scanner scanner = new Scanner(System.in);
	private Disk disk;
	private BPlusTree index;

	/**
	 * 
	 * @param bSize
	 * @throws Exception
	 */
	public void run(int bSize) throws Exception {
		// read records from data file
		List<Record> records = Utility.readRecord(DATA_FILE_PATH);

		disk = new Disk(Constants.DISK_SIZE, bSize);
		index = new BPlusTree(bSize);

		System.out.println("Running program with block size of "+bSize);
		System.out.println("Prepare to insert records into storage and create index");
		Address recordAddr;
		for (Record r: records) {
			// inserting records into disk and create index!
			recordAddr = disk.pushRecord(r);
			index.insert( r.getNumVotes(), recordAddr);
		}
		System.out.println("Record inserted into storage and index created");
		System.out.println("-----------------------------------Experiment 1--------------------------------------------------------");
		disk.diskinfo();
		System.out.println();
		System.out.println("-----------------------------------Experiment 2--------------------------------------------------------");
		index.treeStats();
		System.out.println();


		// Execute the Experiment
		System.out.println("-----------------------------------Experiment 3--------------------------------------------------------");
		doExperiment3();
		System.out.println();
		System.out.println("-----------------------------------Experiment 4--------------------------------------------------------");
		doExperiment4();
		System.out.println();
		System.out.println("-----------------------------------Experiment 5--------------------------------------------------------");
		doExperiment5();
	}

	public void doExperiment3(){
		System.out.println("Experiment 3 started, getting records with numVotes of 500");
		ArrayList<Address> e3RecordAddresses = index.getRecordsWithKey(500);
		ArrayList<Record> records = disk.getRecords(e3RecordAddresses);
		// records collected, do calculate average rating
		double avgRating = 0;
		for (Record record: records) {
			avgRating += record.getAvgRating();
		}
		avgRating /= records.size();
		System.out.println("Average rating="+avgRating);
	}

	public void doExperiment4(){
		System.out.println("Experiment 4 started, getting records with numVotes between 30k-40k ");
		ArrayList<Address> e4RecordAddresses = index.getRecordsWithKeyInRange(30000,40000);
		ArrayList<Record> records = disk.getRecords(e4RecordAddresses);
		// records collected, do calculate average rating
		double avgRating = 0;
		for (Record record: records) {
			avgRating += record.getAvgRating();
		}
		avgRating /= records.size();
		System.out.println("Average rating="+avgRating);
	}

	public void doExperiment5(){
		index.deleteKey(1000);
	}


	// app menu
	private String getOptions(String[] options, boolean includeQuit){
		for (int i = 0; i < options.length; i++) {
			System.out.println(String.format("[%d] %s",i+1, options[i]));
		}
		if (includeQuit){
			System.out.println("[q] quit");
		}
		System.out.print("Enter the option: ");
		return scanner.nextLine();
	}
	private void pause(){
		pause(null);
	}
	private void pause(String message){
		if (message == null){
			message = "Press any key to continue";
		}
		System.out.print(message);
		scanner.nextLine();
	}

	public void displayMainMenu() throws Exception {
		String[] menu = {
				"Experience with block size 200B",
				"Experience with block size 500B",
		};
		String input;
		do {
			System.out.println("-----------------------------------Experiment 4--------------------------------------------------------");
			System.out.println("CZ4031 - Database System Principles Assignment-1 (Group "+GROUP_NUM+")");
			input = getOptions(menu, true);
			switch (input) {
				case "1":
					run(BLOCK_SIZE_200);
					pause();
					break;
				case "2" :
					run(BLOCK_SIZE_500);
					pause();
					break;
			}
		} while (!input.equals("q"));
	}

	public static void main(String[] args) {
		try {
			MainApp app = new MainApp();
			app.displayMainMenu();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}