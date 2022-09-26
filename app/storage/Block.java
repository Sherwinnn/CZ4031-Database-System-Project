package app.storage;


import java.util.Arrays;


public class Block {
    
    /**
     * size of one block
     */
    private int blkSize;
    
    /**
     * list of records in every block
     */
    public Record[] records;

    /**
     * max number of records in each block
     */
    public int maximumRecords;

    /**
     * current number of records in a block
     */
    public int currentRec;


    /**
     * define constructor for an empty block with blocksize as input
     * @param bSize
     */
    public Block(int bSize){
        this.blkSize = bSize;
        this.currentRec = 0;
        this.maximumRecords = bSize / Record.getSize();
        this.records = new Record[maximumRecords];
    }

    /**
     * define constructor for a block with records
     * number of records is updated as well
     * @param bSize
     * @param array of records
     */
    public Block(int bSize, Record[] records){
        this.blkSize = bSize;
        this.records = records;
        this.maximumRecords = bSize/Record.getSize();
        this.currentRec = records.length;
    }

    /**
     * check if the block has space
     */
    public boolean isAvail(){
        if (currentRec < maximumRecords){
            return true;
        }
        else{
            return false;
        }
    }

    /**
     * read record given the id of the record, relative to the starting address of block
     * @param id of the record
     * @return the required record
     */
    public Record readRecord(int id){
        return records[id];
    }

    /** 
     * add record into block 
     * check if block has space first
     * if there is space, find empty space to insert record
     * return the id of this record
     */
    public int addRecord(Record rec) throws Exception {
        if (!isAvail()){
            throw new Exception("Insufficient space in block for record");
        }
        // iterate through the block to check
        for (int i = 0; i < records.length ; i++) {
            if (records[i] == null ){
                records[i] = rec;
                int recordID = i;
                currentRec++; // update num of records in the block
                break;
            }
        }
        return recordID;
    }

    /**
     * remove the record from the block given record id
     * @param id of the record
     * @return true if successfully removed
     */
    public boolean delRecord(int id){
        if (records[id]!=null){
            records[id] = null;
            currentRec--;
            return true;
        }
        return false;
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int i=0; i< data.length; i++){
            if (i>0){
                sb.append(", ");
            }
            sb.append(String.format("%d:{%s}", i, data[i].tconst ));
        }
        sb.append("]");
        return sb.toString();
    }
}