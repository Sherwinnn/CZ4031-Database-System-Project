package databaseApp.dataStorage;

import java.util.*;

import databaseApp.utility.FileUtil;

/**
 * this class represents the disk storage, which stores the blocks of data
 */
public class MainDisk {
    private int diskSize;
    private int blockSize;
    private int recordSize;
    private int recordNum;
    private int maxBlockNum;

    private ArrayList<DataBlock> blocks;

    /**
     * define constructor for disk
     * number of records = 0 at the start
     * calculate number of blocks that can be stored in a disk
     */
    public MainDisk(int dSize, int bSize) {
        this.diskSize = dSize;
        this.blockSize = bSize;
        this.recordNum = 0;
        this.maxBlockNum = diskSize / blockSize;
        this.blocks = new ArrayList<>();
    }

    /**
     * @return number of blocks in disk
     */
    public int getBlockNum() {
        return blocks.size();

    }

    /**
     * @return number of records in the disk
     */
    public int getRecordNum() {
        return recordNum;
    }

    /**
     * @return the amount of disk space used
     */
    public int getUsedDisk() {
        return getBlockNum() * blockSize;
    }

    /**
     * @return the amount of disk space available
     */
    public int getAvailDisk() {
        return diskSize - getUsedDisk();
    }

    /**
     * add a record to a given block id
     * if the block id given is unavailable, try to make a new block
     * 
     * @param blkID
     * @param rec
     * @return address of the added record
     * @throws Exception
     */
    public BlockAddress addRecordTo(int blkID, RecordFile rec) throws Exception {
        DataBlock targetBlk = null;
        if (blkID >= 0) {
            targetBlk = retrieveBlock(blkID);
        }

        if (targetBlk == null || !targetBlk.isAvail()) {
            // target block is unavail, check if disk has additional space for more blocks
            if (blocks.size() == maxBlockNum) {
                throw new Exception("Disk is full!");
            }
            // disk has additional space, create new block
            targetBlk = new DataBlock(blockSize);
            blocks.add(targetBlk);
            blkID = getLastBlockAddress();
        }
        // add record to target block
        int recID = targetBlk.addRecord(rec);
        recordNum++;
        return new BlockAddress(blkID, recID);
    }

    /**
     * Check if there is empty block in the disk
     * if yes, add the record into the first empty block thats available
     * 
     * @param rec to be added
     * @return id of the record
     * @throws Exception
     */
    public BlockAddress addRecord(RecordFile rec) throws Exception {
        int blockId = getFirstAvailBlockAddress();
        return addRecordTo(blockId, rec);
    }

    /**
     * push record into the last block on disk.
     * if current last block is filled, a new block will be created to add record.
     * 
     * @param record to be pushed to the last block
     * @return record id
     * @throws Exception
     */
    public BlockAddress pushRecord(RecordFile rec) throws Exception {
        int blockId = getLastBlockAddress();
        return addRecordTo(blockId, rec);
    }

    /**
     * check for any available blocks and return the first available block id
     * 
     * @return block id of the first block thats available
     */
    public int getFirstAvailBlockAddress() {
        int blkID = 0;
        // iterate through the array of blocks for empty block
        for (int j = 0; j < blocks.size(); j++) {
            if (blocks.get(j).isAvail()) {
                blkID = j;
                return blkID;

            }
        }
        return blkID;
    }

    /**
     * @return address of the last block in the blocks arraylist
     */
    public int getLastBlockAddress() {
        int blkID = 0;
        if (blocks.size() == 0) {
            return -1;
        } else {
            blkID = blocks.size() - 1;
            return blkID;
        }
    }

    /**
     * retrieve a block given its address
     * 
     * @param bID
     * @return block
     */
    public DataBlock retrieveBlock(int bID) {
        return blocks.get(bID);
    }

    /**
     * retrieve a record given its record id and the block id
     * 
     * @param bID
     * @param recordID
     * @return
     */
    public RecordFile retrieveRecord(int bID, int recordID) {
        return retrieveBlock(bID).readRecord(recordID);
    }

    /**
     * retrieve a record given the address
     * 
     * @param address of record
     * @return the record to be retrieved
     */
    public RecordFile retrieveRecord(BlockAddress addr) {
        int blockID = addr.getBID();
        int recordID = addr.getID();
        return retrieveRecord(blockID, recordID);
    }
    /*
     * 
     * retrieve an array of records
     * given an array of record addresses
     * 
     * @param arraylist of addresses
     * 
     * @return arraylist of records
     */

    public ArrayList<RecordFile> getRecords(ArrayList<BlockAddress> addresses) {

        int block_access = 0;
        DataBlock blk = null;
        HashMap<Integer, DataBlock> cache_data = new HashMap<>();
        ArrayList<RecordFile> retrievedRecords = new ArrayList<>();

        for (BlockAddress address : addresses) {
            // look for the block from cache data first
            blk = cache_data.get(address.getBID());
            if (blk == null) {
                blk = retrieveBlock(address.getBID());
                cache_data.put(address.getBID(), blk);
                block_access++;
            }

            RecordFile rec = blk.readRecord(address.getID());
            retrievedRecords.add(rec);
        }
        System.out.printf("Retrieved %d records with %d block access", retrievedRecords.size(), block_access);
        System.out.println();
        return retrievedRecords;
    }

    /**
     * delete record given block id and record id
     * 
     * @param bId
     * @param rID
     * @return
     */
    public boolean delRecord(int bID, int rID) {
        boolean deleted = retrieveBlock(bID).delRecord(rID);
        if (deleted) {
            recordNum--;
            return true;
        } else {
            return false;
        }
    }

    /**
     * delete records when an array of addresses is given
     * 
     * @param recordAddresses
     */
    public void delRecords(ArrayList<BlockAddress> recordAddresses) {
        for (int i = 0; i < recordAddresses.size(); i++) {
            int bID = recordAddresses.get(i).getBID();
            int id = recordAddresses.get(i).getID();
            delRecord(bID, id);
        }
    }

    public void diskinfo() {
        System.out.printf("disk size = %s / %s", FileUtil.formatFileSize(getUsedDisk()),
                FileUtil.formatFileSize(diskSize));
        System.out.println();
        System.out.printf(String.format("block size = %s", FileUtil.formatFileSize(blockSize)));
        System.out.println();
        System.out.printf("blocks = %,d / %,d", blocks.size(), maxBlockNum);
        System.out.println();
        System.out.printf("records = %,d", recordNum);
    }
}