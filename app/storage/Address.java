package app.storage;

/**
 * this class represents the record's address
 */
public class Address {
    /**
     * block id, which the record is stored in
     */
    private int bID;

    /**
     * record id
     */
    private int recID;

    /**
     * this defines a constructor for a record address
     * @param bID
     * @param id
     */
    public Address(int bID, int id){
        this.bID=bID;
        this.recID=id;
    }

    /**
     * setter and getter for block ID
     * @return block id
     */
    public int getBID() {
        return bID;
    }

    public void setBID(int bID) {
        this.bID = bID;
    }

    /** setter and getter for record ID
     * @return record id
     */
    public int getID() {
        return recID;
    }

    public void setID(int id) {
        this.recID = id;
    }

    @Override
    public String toString() {
        return String.format("@%d-%d", bID, recID);
    }
}