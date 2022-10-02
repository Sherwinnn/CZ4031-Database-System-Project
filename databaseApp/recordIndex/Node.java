package databaseApp.recordIndex;

import java.util.ArrayList;

import databaseApp.recordIndex.Internal_N;

public abstract class Node {

    private ArrayList<Integer> keys;
    private Internal_N parent;
    private boolean isLeaf;
    private boolean isRoot;

    public Node() {

        keys = new ArrayList<Integer>();
        isLeaf = false;
        isRoot = false;
    }

    // get whether it is a leaf
    public boolean getIsLeaf() {

        return isLeaf;
    }

    // set whether it is a leaf
    public void setLeaf(boolean isALeaf) {

        isLeaf = true;
    }

    // get whether it is root
    public boolean getIsRoot() {

        return isRoot;
    }

    // set whether it is root
    public void setIsRoot(boolean isARoot) {

        isRoot = isARoot;
    }

    // get node's parent
    public Internal_N getParent() {

        return parent;
    }

    // set node's parent
    public void setParent(Internal_N progenitor) {

        parent = progenitor;
    }

    // get arraylist of all keys
    public ArrayList<Integer> getKeys() {
        return keys;
    }

    // get key at index
    public int getKey(int index) {

        return keys.get(index);
    }

    // add key
    public int addKey(int key) {

        if (this.getKeys().size() == 0) {

            this.keys.add(key);
            return 0;
        }

        int i;
        keys.add(key);
        for (i = keys.size() - 2; i >= 0; i--) {

            if (keys.get(i) <= key) {

                i++;
                keys.set(i, key);
                break;
            }

            keys.set(i + 1, keys.get(i));
            if (i == 0) {

                keys.set(i, key);
                break;
            }
        }

        return i;
    }

    // delete key from index
    public void deleteKey(int index) {

        keys.remove(index);
    }

    // for deleting keys before splitting
    public void deleteKeys() {

        keys = new ArrayList<Integer>();
    }

    // find smallest key (more for use by parentnode but placed here for first level
    // of parents)
    public int findSmallestKey() {

        int key;
        Internal_N copy;

        if (!this.getIsLeaf()) {

            copy = (Internal_N) this;

            while (!copy.getChild(0).getIsLeaf())
                copy = (Internal_N) copy.getChild(0);

            key = copy.getChild(0).getKey(0);
        }

        else
            key = this.getKey(0);

        return key;
    }

    // delete the node
    public void deleteNode() {

        if (parent != null) {

            parent.deleteChild(this);
            parent = null;
        }

        if (this.isLeaf) {

            Leaf_N copy = (Leaf_N) this;
            copy.deleteRecords();
            copy.setNext(null);
        }

        else {

            Internal_N copy = (Internal_N) this;
            copy.deleteChildren();
        }

        isLeaf = false;
        isRoot = false;
        keys = new ArrayList<Integer>();
    }

}