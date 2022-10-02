package databaseApp.dataStorage;

/**
 * this class represents a record that will be stored in blocks on disk
 */

public class RecordFile {

	/**
	 * average rating
	 */
	private float avgRating;

	/**
	 * number of votes
	 */
	private int numVotes;

	/**
	 * assume that a record is 18 bytes
	 * avgRating is 4 bytes
	 * numVotes is 4 bytes
	 * tconst is 10 bytes (a string consisting of 10 characters)
	 */
	private int size = 18;

	/**
	 * title identifier of the movie
	 */
	public String tconst;

	/*
	 * define constructor for the record class
	 * 
	 * public Record(){
	 * this.tconst = new char[10];
	 * // every tconst has max 10 characters based on the data
	 * }
	 */

	/**
	 * define constructor for the record class
	 * 
	 * @param avgRating
	 * @param numVotes
	 * @param tconst
	 */
	public RecordFile(float avgRating, int numVotes, String tconst) {
		this.avgRating = avgRating;
		this.numVotes = numVotes;
		this.tconst = tconst;
	}

	/**
	 * setter and getter for avgRating
	 * 
	 * @return average rating of movie
	 */
	public float getAvgRating() {
		return avgRating;
	}

	public void setAvgRating(float avgRating) {
		this.avgRating = avgRating;
	}

	/**
	 * setter and getter for numVotes
	 * 
	 * @return number of votes
	 */
	public int getNumVotes() {
		return numVotes;
	}

	public void setNumVotes(int numVotes) {
		this.numVotes = numVotes;
	}

	/**
	 * setter and getter for tconst
	 * 
	 * @return title identifier of movie
	 */
	public String getTconst() {
		return tconst;
	}

	public void setTconst(String tconst) {
		this.tconst = tconst;
	}

	/**
	 * @return record size (18 bytes)
	 */
	public static int getSize() {
		return 18;
	}

	@Override
	public String toString() {
		return "{" + tconst + "; " + avgRating + "; " + numVotes + "}";
	}
}