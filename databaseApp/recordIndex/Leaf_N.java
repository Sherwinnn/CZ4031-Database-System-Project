package databaseApp.recordIndex;

import java.util.ArrayList;

import databaseApp.recordIndex.Node;
import databaseApp.dataStorage.BlockAddress;

public class Leaf_N extends Node {
    private ArrayList<BlockAddress> records;
    private Leaf_N next;

    public Leaf_N() {
        super();
        records = new ArrayList<BlockAddress>();
        setLeaf(true);
        setNext(null);
    }

    public ArrayList<BlockAddress> getRecords() {
        return records;
    }

    public BlockAddress getRecord(int index) {
        return records.get(index);
    }

    public Leaf_N getNext() {
        return next;
    }

    public void setNext(Leaf_N sister) {
        next = sister;
    }

    public int addRecord(int key, BlockAddress address) {
        if (this.getRecords().size() == 0) {
            this.records.add(address);
            this.addKey(key);
            return 0;
        }

        records.add(address);

        int index;
        index = super.addKey(key);

        for (int i = records.size() - 2; i >= index; i--)
            records.set(i + 1, records.get(i));
        records.set(index, address);

        return index;
    }

    public void preSplit() {
        deleteKeys();
        records = new ArrayList<BlockAddress>();
    }

    public void deleteRecord(int index) {
        deleteKey(index);
        records.remove(index);
    }

    public void deleteRecords() {
        records = new ArrayList<BlockAddress>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < getKeys().size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(String.format("%d:{%d=>%s}", i, getKey(i), getRecord(i)));
        }
        sb.append("]");
        return sb.toString();
    }
}