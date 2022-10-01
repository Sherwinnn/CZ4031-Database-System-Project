
package app.index;
import app.storage.Address;

import java.util.ArrayList;
import app.index.Node;

public class LeafNode extends Node {
    private ArrayList<Address> records;
    private LeafNode next;

    public LeafNode() {
        super();
        records = new ArrayList<Address>();
        setLeaf(true);
        setNext(null);
    }

    public ArrayList<Address> getRecords() {
        return records;
    }

    public Address getRecord(int index) {
        return records.get(index);
    }

    public LeafNode getNext() {
        return next;
    }

    public void setNext(LeafNode sister) {
        next = sister;
    }

    public int addRecord(int key, Address address) {
        if (this.getRecords().size() == 0) {
            this.records.add(address);
            this.addKey(key);
            return 0;
        }
    
        records.add(address);
    
        int index;
        index = super.addKey(key);

        for (int i = records.size()-2; i >= index; i--) 
            records.set(i+1, records.get(i));
        records.set(index, address);

        return index;
    }

    public void preSplit() {
        deleteKeys();
        records = new ArrayList<Address>();
    }

    public void deleteRecord(int index) {
        deleteKey(index);
        records.remove(index);
    }

    public void deleteRecords() {
        records = new ArrayList<Address>();
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int i=0; i<getKeys().size(); i++){
            if (i>0){
                sb.append(", ");
            }
             sb.append(String.format("%d:{%d=>%s}", i, getKey(i), getRecord(i)));
        }
        sb.append("]");
        return sb.toString();
    }
}