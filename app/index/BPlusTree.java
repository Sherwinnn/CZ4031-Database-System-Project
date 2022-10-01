package app.index;

import app.storage.Address;
import java.util.ArrayList;
import app.index.Node;


public class BPlusTree {
    private static final int SIZE_POINTER = 6; // for 64 bits system
    private static final int SIZE_KEY = 4; // for int
    private int n; // Maximum number of keys
    private int minInternalKeys; // Minimum number of keys for an Internal/Parent Node
    private int minLeafKeys; // Minimum number of keys for an Internal/Parent Node
    private int bPTreeHeight; // Height of B + Tree
    private int nodeCount; // Number of Nodes
    private int nodesRemoved; // Number of nodes removed
    Node root; // New root node assigned for a given B + Tree

    public BPlusTree(int blockSize) {
        this.n = (blockSize - SIZE_POINTER) / (SIZE_KEY + SIZE_POINTER); // Initialisation Constructor for a given block
                                                                         // size
        this.minLeafKeys = (int) Math.floor((n + 1) / 2);
        this.minInternalKeys = (int) Math.floor(n / 2);
        this.nodeCount = 0;
        this.nodesRemoved = 0;
        System.out.println("-------------------------------------------------------------------------------------------------------");
        System.out.println( "init: blockSize = "+blockSize+", maxKeys = "+n);
        System.out.println( "MinKeys: parent="+ minInternalKeys +", leaf="+ minLeafKeys);

        this.root = new LeafNode();
        root.setIsRoot(true);
        bPTreeHeight = 1;
    }

    // to find where to insert
    public LeafNode findLeaf(int key) {
        if (this.root.getIsLeaf())
            return (LeafNode) root;

        ArrayList<Integer> totalKeys;
        ParentNode parent = (ParentNode) root;

        // keep searching and stop before leaf node to find parent
        while (!parent.getChild(0).getIsLeaf()) {
            totalKeys = parent.getKeys();

            // start from the back
            for (int i = totalKeys.size() - 1; i >= 0; i--) {
                if (totalKeys.get(i) <= key) {
                    parent = (ParentNode) parent.getChild(i + 1);
                    break;
                } else if (i == 0)
                    parent = (ParentNode) parent.getChild(0);
            }
        }

        // find the key itself
        totalKeys = parent.getKeys();
        for (int i = totalKeys.size() - 1; i >= 0; i--) {
            if (totalKeys.get(i) <= key)
                return (LeafNode) parent.getChild(i + 1);
        }

        return (LeafNode) parent.getChild(0);
    }

    // to insert a record into the tree
    public void insert(int key, Address address) {
        // to find where to insert to
        LeafNode found = this.findLeaf(key);

        if (found.getKeys().size() < n)
            found.addRecord(key, address);
        // exceed n, need to split
        else {
            int totalKeys[] = new int[n + 1];
            Address address_list[] = new Address[n + 1];
            LeafNode newLeaf = new LeafNode();

            for (int i = 0; i < n; i++) {
                totalKeys[i] = found.getKey(i);
                address_list[i] = found.getRecord(i);
            }

            // sort
            for (int i = n - 1; i >= 0; i--) {
                if (totalKeys[i] <= key) {
                    i++;
                    totalKeys[i] = key;
                    address_list[i] = address;
                    break;
                }
                totalKeys[i + 1] = totalKeys[i];
                address_list[i + 1] = address_list[i];
            }

            // delete values
            found.preSplit();

            // putting the keys and addresses into the two leafnodes
            for (int i = 0; i < minLeafKeys; i++)
                found.addRecord(totalKeys[i], address_list[i]);
            for (int i = minLeafKeys; i < n + 1; i++)
                newLeaf.addRecord(totalKeys[i], address_list[i]);

            // linking node
            newLeaf.setNext(found.getNext());
            found.setNext(newLeaf);

            // edit parents if needed
            if (found.getIsRoot()) {
                found.setIsRoot(false);

                ParentNode newRoot = new ParentNode();
                newRoot.addChild(found);
                newRoot.setIsRoot(true);
                newRoot.addChild(newLeaf);
                root = newRoot;

                bPTreeHeight++;
            } else if (found.getParent().getKeys().size() < n)
                found.getParent().addChild(newLeaf);
            else
                splitParent(found.getParent(), newLeaf);

            // updating nodeCount
            nodeCount++;
        }
    }

    // to split a full parent node, similar to spliting node
    public void splitParent(ParentNode parent, Node child) {
        Node children[] = new Node[n + 2];
        int totalKeys[] = new int[n + 2];
        int smallestkey = child.findSmallestKey();
        ParentNode newParent = new ParentNode();

        for (int i = 0; i < n + 1; i++) {
            children[i] = parent.getChild(i);
            totalKeys[i] = children[i].findSmallestKey();
        }

        for (int i = n; i >= 0; i--) {
            if (totalKeys[i] <= smallestkey) {
                i++;
                totalKeys[i] = smallestkey;
                children[i] = child;
                break;
            }
            totalKeys[i + 1] = totalKeys[i];
            children[i + 1] = children[i];
        }

        parent.preSplit();

        for (int i = 0; i < minInternalKeys + 2; i++)
            parent.addChild(children[i]);
        for (int i = minInternalKeys + 2; i < n + 2; i++)
            newParent.addChild(children[i]);

        if (parent.getIsRoot()) {
            ParentNode newRoot = new ParentNode();
            newRoot.setIsRoot(true);
            newRoot.addChild(parent);
            newRoot.addChild(newParent);
            root = newRoot;

            parent.setIsRoot(false);

            bPTreeHeight++;
        } else if (parent.getParent().getKeys().size() < n)
            parent.getParent().addChild(newParent);
        else
            splitParent(parent.getParent(), newParent);

        // update nodeCount
        nodeCount++;
    }

    // delete all records of a key
    public void deleteKey(int key) {
        ArrayList<Integer> totalKeys;
        LeafNode leaf;

        // make sure there is no repeat
        while (findRecordsWithKey(key).size() != 0) {
            leaf = findLeaf(key);
            totalKeys = leaf.getKeys();

            for (int i = 0; i < totalKeys.size(); i++) {
                if (totalKeys.get(i) == key) {
                    leaf.deleteRecord(i);
                    // need to update tree
                    if (!leaf.getIsRoot())
                        balanceLeaf(leaf);

                    break;
                }
            }
        }

        System.out.println("Number of Nodes Deleted ="+nodesRemoved);
        nodeCount -= nodesRemoved;
        treeStats();
    }

    // find address of records for a given key
    public ArrayList<Address> findRecordsWithKey(int key) {
        ArrayList<Address> result = new ArrayList<>();
        Node curr = root;
        ParentNode parent;

        // search leaf nodes
        while (!curr.getIsLeaf()) {
            parent = (ParentNode) curr;
            for (int i = 0; i < parent.getKeys().size(); i++) {
                if (key <= parent.getKey(i)) {
                    curr = parent.getChild(i);
                    break;
                }
                if (i == parent.getKeys().size() - 1) {
                    curr = parent.getChild(i + 1);
                    break;
                }
            }
        }

        // find the records
        LeafNode curLeaf = (LeafNode) curr;
        boolean finished = false;

        // check if there is same key
        while (!finished && curLeaf != null) {
            for (int i = 0; i < curLeaf.getKeys().size(); i++) {
                if (curLeaf.getKey(i) == key) {
                    result.add(curLeaf.getRecord(i));
                    continue;
                }
                if (curLeaf.getKey(i) > key) {
                    finished = true;
                    break;
                }
            }

            // move on to sibling node
            if (!finished) {
                if (curLeaf.getNext() != null) {
                    curLeaf = curLeaf.getNext();
                } else
                    break;
            }
        }
        return result;
    }

    public void balanceLeaf(Node x) {
        if(x.getIsLeaf()){
            LeafNode node = (LeafNode) x;
            int need = minLeafKeys - node.getKeys().size();
            LeafNode prev = (LeafNode) node.getParent().getPrev(node);
            LeafNode next = (LeafNode) node.getParent().getAfter(node);
            int nextAvail = 0;
            int prevAvail = 0;
            ParentNode temp;

            // check if can borrow from prev or next nodes
            if (next != null)
                nextAvail += next.getKeys().size() - minLeafKeys;
            if (prev != null)
                prevAvail += prev.getKeys().size() - minLeafKeys;

            // need to merge
            if (need > nextAvail + prevAvail) {
                if (prev != null && next != null) {
                    for (int i = 0; i < n - (prevAvail + minLeafKeys); i++)
                        prev.addRecord(node.getKey(i), node.getRecord(i));
                    for (int i = n - (prevAvail + minLeafKeys); i < node.getKeys().size(); i++)
                        next.addRecord(node.getKey(i), node.getRecord(i));
                }

                // if only has next node
                else if (prev == null) {
                    for (int i = 0; i < node.getKeys().size(); i++)
                        next.addRecord(node.getKey(i), node.getRecord(i));
                }

                // if node has prev node
                else {
                    for (int i = 0; i < node.getKeys().size(); i++)
                        prev.addRecord(node.getKey(i), node.getRecord(i));
                }

                // need to delete next
                temp = node.getParent();

                if (prev == null) {
                    if (!temp.getIsRoot())
                        prev = findLeaf(temp.findSmallestKey() - 1);
                }

                prev.setNext(node.getNext());
                node.deleteNode();
                nodesRemoved++;
            }

            // can borrow
            else {
                if (prev == null) {
                    for (int i = 0; i < need; i++) {
                        node.addRecord(next.getKey(i), next.getRecord(i));
                        next.deleteRecord(i);
                    }
                }

                else if (prev != null && next != null) {

                    // borrow from prev (as many as possible)
                    for (int i = 0; i < prevAvail; i++) {
                        node.addRecord(prev.getKey(prev.getKeys().size() - 1 - i),
                                prev.getRecord(prev.getKeys().size() - 1 - i));
                        prev.deleteRecord(prev.getKeys().size() - 1 - i);
                    }

                    // take from next node
                    for (int i = prevAvail, j = 0; i < need; i++, j++) {
                        node.addRecord(next.getKey(j), next.getRecord(j));
                        next.deleteRecord(j);
                    }
                }
                
                else {
                    for (int i = 0; i < need; i++) {
                        node.addRecord(prev.getKey(prev.getKeys().size() - 1 - i),
                                prev.getRecord(prev.getKeys().size() - 1 - i));
                        prev.deleteRecord(prev.getKeys().size() - 1 - i);
                    }
                }

                temp = node.getParent();
            }
            balanceLeaf(temp);
        }else{
            ParentNode parent = (ParentNode) x;
            if (parent.getIsRoot()) {
                if (parent.getChildren().size() > 1) {
                    Node child = parent.getChild(0);
                    parent.deleteChild(child);
                    parent.addChild(child);
                    return;
                }
                // only has one child
                else {
                    parent.getChild(0).setIsRoot(true);
                    root = parent.getChild(0);
                    parent.deleteNode();
                    nodesRemoved++;
                    bPTreeHeight--;
                    return;
                }
            }
    
            ParentNode prev = (ParentNode) parent.getParent().getPrev(parent);
            ParentNode next = (ParentNode) parent.getParent().getAfter(parent);
            int need = minInternalKeys - parent.getKeys().size();
            int prevAvail = 0;
            int nextAvail = 0;
            ParentNode temp;
    
            if (next != null)
                nextAvail += next.getKeys().size() - minInternalKeys;
    
            if (prev != null)
                prevAvail += prev.getKeys().size() - minInternalKeys;
            //if merging is required
            if (need > nextAvail + prevAvail) {
                if (prev != null && next != null) {
                    for (int i = n - (prevAvail + minInternalKeys) + 1; i < parent.getChildren().size(); i++)
                        next.addChild(parent.getChild(i));
                    for (int i = 0; i < n - (prevAvail + minInternalKeys) + 1 && i < parent.getChildren().size(); i++)
                        prev.addChild(parent.getChild(i));
    
                }
    
                else if (prev != null) {
                    for (int i = 0; i < parent.getChildren().size(); i++)
                        prev.addChild(parent.getChild(i));
    
                } else {
                    for (int i = 0; i < parent.getChildren().size(); i++)
                        next.addChild(parent.getChild(i));
    
                }
    
                temp = parent.getParent();
                parent.deleteNode();
                nodesRemoved++;
            }
            //Borrow of Keys
            else {
                if (prev != null && next != null) {
                    for (int i = 0; i < prevAvail && i < need; i++) {
                        parent.addChild(prev.getChild(prev.getChildren().size() - 1), 0);
                        prev.deleteChild(prev.getChild(prev.getChildren().size() - 1));
                    }
                    for (int i = prevAvail; i < need; i++) {
                        parent.addChild(next.getChild(0));
                        next.deleteChild(next.getChild(0));
                    }
                }
    
                else if (prev == null) {
                    for (int i = 0; i < need; i++) {
                        parent.addChild(next.getChild(0));
                        next.deleteChild(next.getChild(0));
                    }
                } else {
                    for (int i = 0; i < need; i++) {
                        parent.addChild(prev.getChild(prev.getChildren().size() - 1 - i), 0);
                        prev.deleteChild(prev.getChild(prev.getChildren().size() - 1 - i));
                    }
                }
                temp = parent.getParent();
            }
            balanceLeaf(temp);
        }
    }

    public ArrayList<Address> getRecordsWithKey(int key) {
        ArrayList<Address> result = new ArrayList<>();
        ArrayList<Integer> content = new ArrayList<>();
        int blockAccess = 1;
        int siblingAccess = 0;

        System.out.println("Accessing root node");

        Node curr = root;
        ParentNode parentNode;

        int k = 0;
        while(k<curr.getKeys().size()){
            content.add(curr.getKey(k));
            k++;
        }
        System.out.println("Content of root node: "+ content);

        // search and stop before leaf node
        while (!curr.getIsLeaf()) {
            parentNode = (ParentNode) curr;
            for (int i = 0; i < parentNode.getKeys().size(); i++) {
                if (key <= parentNode.getKey(i)) {
                    System.out.println("Follwing the Pointer of :" + i + ": key " + key  + " <=curKey " + parentNode.getKey(i));
                    curr = parentNode.getChild(i);
                    blockAccess++;
                    if (blockAccess <= 5) {
                        content.removeAll(content);
                        int j = 0;
                        while(j<curr.getKeys().size()){
                            content.add(curr.getKey(j));
                            j++;
                        }
                        System.out.println("Content of current node: "+ content);
                    }
                    break;
                }
                if (i == parentNode.getKeys().size() - 1) {

                    curr = parentNode.getChild(i + 1);
                    blockAccess++;
                    if (blockAccess <= 5) {
                        content.removeAll(content);
                        int j = 0;
                        while(j< curr.getKeys().size()){
                            content.add(curr.getKey(j));
                            j++;
                        }
                        System.out.println("Content of current node: "+ content);
                    }
                    break;
                }
            }
        }

        LeafNode curLeaf = (LeafNode) curr;
        boolean finished = false;

        while (!finished && curLeaf != null) {
            for (int i = 0; i < curLeaf.getKeys().size(); i++) {
                // find same key
                if (curLeaf.getKey(i) == key) {
                    result.add(curLeaf.getRecord(i));
                    continue;
                }
                if (curLeaf.getKey(i) > key) {
                    finished = true;
                    break;
                }
            }
            // check next node
            if (!finished) {
                if (curLeaf.getNext() != null) {
                    curLeaf = curLeaf.getNext();
                    blockAccess++;
                    siblingAccess++;
                } else {
                    break;
                }
            }
        }

        System.out.println( blockAccess + " nodes (including sibling nodes) accessed");

        if (siblingAccess > 0){
                System.out.println( siblingAccess + " sibling nodes accessed");
            }
        
        else{
            System.out.println("B+Tree.KeySearch: " + "input " + key +" :" +result.size() + " records found with " + blockAccess +  " node access");
        }
        return result;
        
    }

    public void treeStats() {

        ArrayList<Integer> rootContents = new ArrayList<Integer>();
        ArrayList<Integer> firstContents = new ArrayList<Integer>();
        ParentNode rootCopy = (ParentNode) root;
        Node first = rootCopy.getChild(0);

        int i = 0;
        while(i< root.getKeys().size()){
            rootContents.add(root.getKey(i));
            i++;
        }

        int j = 0;
        while(j<first.getKeys().size()){
            firstContents.add(first.getKey(j));
            j++;
        }
        System.out.println("B+ Tree Information:");
        System.out.println( "n = " + n + ", number of nodes = " + nodeCount + ", height = " + bPTreeHeight);
        System.out.println("root node ccontents = " + rootContents);
        System.out.println("First Child Contents = "+ firstContents);
    }

    // To be ussed in main
    // For Experiment 4
    public ArrayList<Address> getRecordsWithKeyInRange(int min, int max) {
        return getRecordsWithKeyInRange(min, max, true);
    }

    
    public ArrayList<Address> getRecordsWithKeyInRange(int min, int max, boolean isVerbose) {
        ArrayList<Address> result = new ArrayList<>();
        ArrayList<Integer> content = new ArrayList<>();
        int nodeAccess = 1; // access the root??
        int siblingAccess = 0;
        if (isVerbose) {
            System.out.println("Access Root Node");
        }

        Node curr = root;

        int k = 0;
        while(k<curr.getKeys().size()){
            content.add(curr.getKey(k));
            k++;
        }
        System.out.println("Content of root node: "+ content);

        ParentNode parentNode;
        // searching for leaf node with key
        while (!curr.getIsLeaf()) {
            parentNode = (ParentNode) curr;
            for (int i = 0; i <= parentNode.getKeys().size(); i++) {
                if (min <= parentNode.getKey(i)) {
                    curr = parentNode.getChild(i);
                    nodeAccess++;
                    if (isVerbose) {
                        System.out.println("Following pointer " + i + ": min " + min + " = curKey " + parentNode.getKey(i));
                        if (nodeAccess <= 5) {
                            content.removeAll(content);
                            int j = 0;
                            while(j<curr.getKeys().size()){
                                content.add(curr.getKey(j));
                                j++;
                            }
                            System.out.println("Content of current node: "+ content);
                        }
                    }
                    break;
                }
                if (i == parentNode.getKeys().size() - 1) {
                    curr = parentNode.getChild(i + 1);
                    nodeAccess++;
                    if (isVerbose) {
                        System.out.println("Following pointer " + i +"+1" + ": last key and min " +min + " > curKey " + parentNode.getKey(i));
                        if (nodeAccess <= 5) {
                            content.removeAll(content);
                            int j = 0;
                            while(j<curr.getKeys().size()){
                                content.add(curr.getKey(j));
                                j++;
                            }
                            System.out.println("Content of current node: "+ content);
                        }
                    }
                    break;
                }
            }
        }
        // next leaf node is found, find all records with same key
        LeafNode curLeaf = (LeafNode) curr;
        boolean finished = false;
        while (!finished && curLeaf != null) {
            // finding same keys within leaf node
            for (int i = 0; i < curLeaf.getKeys().size(); i++) {
                // found same key, add into result list
                if (curLeaf.getKey(i) >= min && curLeaf.getKey(i) <= max) {
                    result.add(curLeaf.getRecord(i));
                    continue;
                }
                // if curKey > searching key, no need to continue searching
                if (curLeaf.getKey(i) > max) {
                    finished = true;
                    break;
                }
            }
            if (!finished) {
                // trying to check sibling node has remaining records of same key
                if (curLeaf.getNext() != null) {
                    curLeaf = (LeafNode) curLeaf.getNext();
                    nodeAccess++;
                    siblingAccess++;
                } else {
                    break;
                }
            }
        }
        if (siblingAccess > 0) {
            if (isVerbose) {
                System.out.println( siblingAccess + " sibling node accessed");
            }
        }
        if (isVerbose) {
            System.out.println("Input ("+ min + "," +max + "): " + result.size() + " records found with " + nodeAccess + " node access");

        }
        return result;
    }

    
    public ArrayList<Address> removeRecordsWithKey() {
        // list of address need to be return, so app can use it to delete records from
        // disk
        return null;
    }

    
}

