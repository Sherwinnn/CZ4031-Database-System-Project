package app;
 
import java.util.*;

import app.storage.Disk;
import app.storage.Record;
import app.util.Utility;
import app.index.BPlusTree;
import app.storage.Address;

public class MainApp implements Constants {
	
	private Disk disk;
	private BPlusTree index;


	Scanner scanner = new Scanner(System.in);

	/**
	 * 
	 * @param bSize
	 * @throws Exception
	 */

	public void run(int bSize) throws Exception {
		// read records from data file
		List<Record> records = Utility.readRecord(DATA_PATH);

		disk = new Disk(Constants.DISK_SIZE, bSize);
		index = new BPlusTree(bSize);

		System.out.println("The program is reading data and running with the following block size : "+bSize);
		System.out.println("The records are inserted into disk and the B+ tree is being created.");
		Address recordAddress;
		for (Record r: records) {
			// inserting records into disk and create index!
			recordAddress = disk.pushRecord(r);
			index.insert( r.getNumVotes(), recordAddress);
		}
		
		System.out.println("The records have been inserted into storage and the B+ tree has been created.");
		System.out.println("-----------------------------------Experiment 1--------------------------------------------------------");
		disk.diskinfo();
		System.out.println();
		System.out.println("-----------------------------------Experiment 2--------------------------------------------------------");
		index.treeStats();
		System.out.println();


		// Execute the Experiments
		System.out.println("-----------------------------------Experiment 3--------------------------------------------------------");
		doExperiment3();
		System.out.println();
		System.out.println("-----------------------------------Experiment 4--------------------------------------------------------");
		doExperiment4();
		System.out.println();
		System.out.println("-----------------------------------Experiment 5--------------------------------------------------------");
		doExperiment5();
	}

	//Conduct Experiment 3 ->Records with NumVotes of 500
	public void doExperiment3(){
		System.out.println("To Do: Retrieve movies with numvotes = 500.");
		ArrayList<Address> RcdAddress = index.getRecordsWithKey(500);
		ArrayList<Record> records = disk.getRecords(RcdAddress);

		for(int i  = 0;i<5;i++){
			Address address = RcdAddress.get(i);
			int b = address.getBID();
			System.out.println("\nBlock "+ b + ": ");
			for(int j = 0;j<5;j++){
			  Record x = disk.retrieveBlock(b).readRecord(j);
			  System.out.print(x.getTconst()+" ");
			}
		}

		//Collecting Records and Calculating AvgRating
		double avgRating = 0;

		for (Record record: records) {
			avgRating = avgRating + record.getAvgRating();
		}
		avgRating = avgRating / records.size(); //Cal AvgRating -> Total Average Rating/ Total Record Size 
		System.out.println("\nAverage rating="+avgRating);
	}

	//Conduct Experiment 4 -> Records with numVotes 30k-40k
	public void doExperiment4(){
		System.out.println("To Do: Retrieve Movies with NumVotes from 30,000 - 40,000. ");
		ArrayList<Address> RcdAddress = index.getRecordsWithKeyInRange(30000,40000);
		ArrayList<Record> records = disk.getRecords(RcdAddress);
		// records collected, do calculate average rating

		for(int i  = 0;i<5;i++){
			Address address = RcdAddress.get(i);
			int b = address.getBID();
			System.out.println("\nBlock "+ b + ": ");
			for(int j = 0;j<5;j++){
			  Record x = disk.retrieveBlock(b).readRecord(j);
			  System.out.print(x.getTconst()+" ");
			}
		}

		double avgRating = 0;
		for (Record record: records) {
			avgRating = avgRating + record.getAvgRating(); //total avgRating
		}
		//cal avgRating
		avgRating = avgRating / records.size();
		System.out.println("\nAverage rating="+avgRating);
	}

	public void doExperiment5(){
		System.out.println("To Do: Delete  Movies with NumVotes = 1000");
		index.deleteKey(1000);
	}

	// Start Menu
	private String getMenu(String[] options, boolean Quit){
		int i = 0;
		while (i < options.length){
			System.out.println(String.format("[%d] %s",i+1, options[i]));
			i++;
		}
		if (Quit){
			System.out.println("Type 'q' to quit the application");
		}
		System.out.print("Enter the option: ");
		return scanner.nextLine();
	}
	private void pause(){
		pause(null);
	}
	private void pause(String message){
		if (message == null){
			message = "Press 'Enter' to Continue the Operation";
		}
		System.out.print(message);
		scanner.nextLine();
	}

	public void displayMainMenu() throws Exception {
		String[] menu = {
				"Experiment with block size 200B",
				"Experiment with block size 500B",
		};
		String input;
		do {
			System.out.println("------------------------------------------------------------------------------------------------------");
			System.out.println("CZ4031 - Database System Principles Assignment-1 (Group "+GROUP_NUM+")");
			input = getMenu(menu, true);
			switch (input) {
				case "1":
					run(BLOCK_200);
					pause();
					break;
				case "2" :
					run(BLOCK_500);
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