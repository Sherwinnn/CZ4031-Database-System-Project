package databaseApp.recordIndex;

import java.util.ArrayList;

public class Internal_N extends Node {

    private ArrayList<Node> children;

    public Internal_N() {
        super();
        children = new ArrayList<Node>();
    }

    public ArrayList<Node> getChildren() {
        return children;
    }

    public Node getChild(int index) {
        return children.get(index);
    }

    public int addChild(Node child) {
        if (children.size() == 0) {
            children.add(child);
            child.setParent(this);
            return 0;
        }

        int key = child.findSmallestKey();
        int smallest = this.findSmallestKey();
        int index;

        if (key < smallest) {
            this.addKey(smallest);
            this.children.add(0, child);
            index = 0;
        }

        else {
            index = this.addKey(key);
            this.children.add(index + 1, child);
        }

        child.setParent(this);
        return index;
    }

    public void addChild(Node child, int index) {
        children.add(0, child);
        child.setParent(this);
        deleteKeys();

        for (int i = 0; i < children.size(); i++) {

            if (i != 0)
                addKey(children.get(i).findSmallestKey());
        }
    }

    public void preSplit() {
        deleteKeys();
        children = new ArrayList<Node>();
    }

    public void deleteChild(Node child) {
        children.remove(child);
        deleteKeys();

        for (int i = 0; i < children.size(); i++) {
            if (i != 0)
                addKey(children.get(i).findSmallestKey());
        }
    }

    public void deleteChildren() {
        children = new ArrayList<Node>();
    }

    public Node getPrev(Node node) {
        if (children.indexOf(node) != 0)
            return children.get(children.indexOf(node) - 1);
        return null;
    }

    public Node getAfter(Node node) {
        if (children.indexOf(node) != children.size() - 1)
            return children.get(children.indexOf(node) + 1);
        return null;
    }

    void treeStructure() {
        System.out.println(this.toString());

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < getKeys().size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(String.format("%d:{%d}", i, getKey(i)));
        }
        sb.append("]");
        return sb.toString();
    }

}