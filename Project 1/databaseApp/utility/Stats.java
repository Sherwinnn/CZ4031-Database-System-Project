package databaseApp.utility;

import databaseApp.dataStorage.RecordFile;

public class Stats {

    public static int maxVotes = Integer.MIN_VALUE;
    public static int minVotes = Integer.MAX_VALUE;
    public static double maxAvgRating = Double.MIN_VALUE;
    public static double minAvgRating = Double.MAX_VALUE;
    public static int minKeyLength = Integer.MAX_VALUE;
    public static int maxKeyLength = Integer.MIN_VALUE;

    public static void analysis(RecordFile record) {
        if (record.getTconst().length() < minKeyLength) {
            minKeyLength = record.getTconst().length();
        }
        if (record.getTconst().length() > maxKeyLength) {
            maxKeyLength = record.getTconst().length();
        }

        if (record.getAvgRating() > maxAvgRating) {
            maxAvgRating = record.getAvgRating();
        }
        if (record.getAvgRating() < minAvgRating) {
            minAvgRating = record.getAvgRating();
        }

        if (record.getNumVotes() > maxVotes) {
            maxVotes = record.getNumVotes();
        }
        if (record.getNumVotes() < minVotes) {
            minVotes = record.getNumVotes();
        }
    }

    public static void overallinfo() {
        System.out.printf("KeyLength = %d - %d", minKeyLength, maxKeyLength);
        System.out.println();
        System.out.printf("avgRating= %.2f - %.2f", minAvgRating, maxAvgRating);
        System.out.println();
        System.out.printf("numVotes= %,d - %,d", minVotes, maxVotes);
        System.out.println();
    }

    public static void reset() {
        minKeyLength = Integer.MAX_VALUE;
        maxKeyLength = Integer.MIN_VALUE;
        maxAvgRating = Double.MIN_VALUE;
        minAvgRating = Double.MAX_VALUE;
        maxVotes = Integer.MIN_VALUE;
        minVotes = Integer.MAX_VALUE;
    }

}