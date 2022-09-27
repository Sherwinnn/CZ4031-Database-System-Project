package app.index;

import app.storage.Address;
import app.util.Log;
import java.util.ArrayList;

public class BPlusTree {
    private static final String TAG = "B+Tree";
    private static final int SIZE_POINTER = 6; //for 64 bits system
    private static final int SIZE_KEY = 4; //for int
    int n, parentMinKeys,leafMinKeys, height, noOfNode, nodeDeleted;
    Node root;

    public BPlusTree(int blockSize){
        n = (blockSize-SIZE_POINTER) / (SIZE_KEY+SIZE_POINTER);
        leafMinKeys = (int) Math.floor((n+1)/2);
        parentMinKeys = (int) Math.floor(n/2);
        noOfNode = 0;
        nodeDeleted = 0;

        root = new LeafNode();
        root.setIsRoot(true);
        height = 1;

        Log.i(TAG, "initialization: blockSize = " + blockSize + ", maximum number of keys = " + n);
        Log.i(TAG, "minimum number of keys: parent = " + parentMinKeys+ ", leaf = " + leafMinKeys);
    }

    //to find where to insert
    public LeafNode findLeaf(int key) {
        if (this.root.getIsLeaf())
            return (LeafNode) root;

        ArrayList<Integer> keys;
        ParentNode parent = (ParentNode) root;

        //keep searching and stop before leaf node to find parent
        while (!parent.getChild(0).getIsLeaf()) {
            keys = parent.getKeys();

            //start from the back
            for (int i = keys.size()-1; i >= 0; i--) {
                if (keys.get(i) <= key) {
                    parent = (ParentNode) parent.getChild(i+1);
                    break;
                }
                else if (i == 0)
                    parent = (ParentNode) parent.getChild(0);
            }
        }

        //find the key itself
        keys = parent.getKeys();
        for (int i = keys.size()-1; i >= 0; i--) {
            if (keys.get(i) <= key)
                return (LeafNode) parent.getChild(i+1);
        }

        return (LeafNode) parent.getChild(0);
    }

    // to insert a record into the tree
    public void insert(int key, Address address) {
        //to find where to insert to
        LeafNode found = this.findLeaf(key);

        if (found.getKeys().size() < n) 
            found.addRecord(key, address);
        //exceed n, need to split
        else {
            int keys[] = new int[n+1];
            Address address_list[] = new Address[n+1];
            LeafNode newLeaf = new LeafNode();

            for (int i = 0; i < n; i++) {
                keys[i] = found.getKey(i);
                address_list[i] = found.getRecord(i);
            }
        
            //sort
            for (int i = n-1; i >= 0; i--) {
                 if (keys[i] <= key) {
                    i++;
                    keys[i] = key;
                     address_list[i] = address;
                     break;
                }
                keys[i+1] = keys[i];
                address_list[i+1] = address_list[i];
            } 
    
            //delete values
            found.preSplit();
    
            //putting the keys and addresses into the two leafnodes
            for (int i = 0; i < leafMinKeys; i++) 
                found.addRecord(keys[i], address_list[i]);
            for (int i = leafMinKeys; i < n+1; i++) 
                newLeaf.addRecord(keys[i], address_list[i]);
    
            //linking node
            newLeaf.setNext(found.getNext());
            found.setNext(newLeaf);
    
            //edit parents if needed
            if (found.getIsRoot()) {
                found.setIsRoot(false);
                
                ParentNode newRoot = new ParentNode();
                newRoot.addChild(found);
                newRoot.setIsRoot(true);
                newRoot.addChild(newLeaf);
                root = newRoot;
                
                height++;
            }
            else if (found.getParent().getKeys().size() < n)
                found.getParent().addChild(newLeaf);
            else 
                splitParent(found.getParent(), newLeaf);
    
            // updating noOfNode
            noOfNode++;
        }
    }

    //to split a full parent node, similar to spliting node
    public void splitParent(ParentNode parent, Node child) {
        Node children[] = new Node[n+2];
        int keys[] = new int[n+2];
        int key = child.findSmallestKey();
        ParentNode newParent = new ParentNode();

        for (int i = 0; i < n+1; i++)  {
            children[i] = parent.getChild(i);
            keys[i] = children[i].findSmallestKey();
        }
        
        for (int i = n; i >= 0; i--) {
            if (keys[i] <= key) {
                i++;
                keys[i] = key;
                children[i] = child;
                break;
            }
            keys[i+1] = keys[i];
            children[i+1] = children[i];
        }

        parent.preSplit();

        for (int i = 0; i < parentMinKeys+2; i++) 
            parent.addChild(children[i]);
        for (int i = parentMinKeys+2; i < n+2; i++) 
            newParent.addChild(children[i]);

        if (parent.getIsRoot()) {
            ParentNode newRoot = new ParentNode();
            newRoot.setIsRoot(true);
            newRoot.addChild(parent);
            newRoot.addChild(newParent);
            root = newRoot;

            parent.setIsRoot(false);

            height++;
        }
        else if (parent.getParent().getKeys().size() < n)
            parent.getParent().addChild(newParent);
        else 
            splitParent(parent.getParent(), newParent);

        //update noOfNode
        noOfNode++;
    }

    //delete all records of a key
    public void deleteKey(int key) {
        ArrayList<Integer> keys;
        LeafNode leaf;

        //make sure there is no repeat
        while (findRecordsWithKey(key).size() != 0) {
            leaf = findLeaf(key);
            keys = leaf.getKeys();

            for (int i = 0; i < keys.size(); i++) {
                if (keys.get(i) == key) {
                    leaf.deleteRecord(i);
                    //need to update tree
                    if (!leaf.getIsRoot())
                        resetLeaf(leaf);
                    break;
                }
            }
        }

        noOfNode -= nodeDeleted;
        treeStats();
        
        Log.d("deletion: ", "number of nodes deleted = " + nodeDeleted);
    }

    //find the where the records are
    public ArrayList<Address> findRecordsWithKey(int key) {
        ArrayList<Address> result = new ArrayList<>();
        Node curr = root;
        ParentNode parent;

        //search leaf nodes
        while (!curr.getIsLeaf()){
            parent = (ParentNode) curr;
            for (int i=0; i<parent.getKeys().size(); i++) {
                if ( key <= parent.getKey(i)) {
                    curr = parent.getChild(i);
                    break;
                }
                if (i == parent.getKeys().size()-1) {
                    curr = parent.getChild(i+1);
                    break;
                }
            }
        }
        
        //find the records
        LeafNode curLeaf = (LeafNode) curr;
        boolean finished = false;

        //check if there is same key
        while(!finished && curLeaf != null){
            for (int i=0; i<curLeaf.getKeys().size(); i++){
                if (curLeaf.getKey(i) == key){
                    result.add(curLeaf.getRecord(i));
                    continue;
                }
                if (curLeaf.getKey(i) > key){
                    finished = true;
                    break;
                }
            }

            //move on to sibling node
            if (!finished){
                if (curLeaf.getNext() != null){
                    curLeaf = curLeaf.getNext();
                }
                else
                    break;
            }
        }
        return result;
    }

    // to update leafnode (see if can borrow and if need to merge)
    public void resetLeaf(LeafNode node) {
        int need = leafMinKeys - node.getKeys().size();
        LeafNode prev = (LeafNode) node.getParent().getPrev(node);
        LeafNode next = (LeafNode) node.getParent().getAfter(node);
        int nextAvail = 0;
        int prevAvail = 0;
        ParentNode temp;

        //check if can borrow from prev or next nodes
        if (prev != null)
            prevAvail += prev.getKeys().size() - leafMinKeys;
        if (next != null) 
            nextAvail += next.getKeys().size() - leafMinKeys;

        //need to merge
        if (need > nextAvail + prevAvail) {
            if (prev != null && next != null) {
                for (int i = 0; i < n-(prevAvail+leafMinKeys); i++) 
                    prev.addRecord(node.getKey(i), node.getRecord(i));
                for (int i = n-(prevAvail+leafMinKeys); i < node.getKeys().size(); i++) 
                    next.addRecord(node.getKey(i), node.getRecord(i));
            }

            //if only has next node
            else if (prev == null) {
                for (int i = 0; i < node.getKeys().size(); i++) 
                    next.addRecord(node.getKey(i), node.getRecord(i));
            }

            //if node has prev node
            else {
                for (int i = 0; i < node.getKeys().size(); i++) 
                    prev.addRecord(node.getKey(i), node.getRecord(i));
            }

            //need to delete next
            temp = node.getParent();

            if (prev == null) {
                if (!temp.getIsRoot())
                    prev = findLeaf(temp.findSmallestKey()-1);
            }

            prev.setNext(node.getNext());
            node.deleteNode();
            nodeDeleted++;
        }

        //can borrow
        else {
            if (prev != null && next != null) {

                //borrow from prev (as many as possible)
                for (int i = 0; i < prevAvail; i++) {
                    node.addRecord(prev.getKey(prev.getKeys().size()-1 -i), prev.getRecord(prev.getKeys().size()-1 -i));
                    prev.deleteRecord(prev.getKeys().size()-1 -i);
                }
                
                //take from next node
                for (int i = prevAvail, j = 0; i < need; i++, j++) {
                    node.addRecord(next.getKey(j), next.getRecord(j));
                    next.deleteRecord(j);
                }
            }

            else if (prev == null) {
                for (int i = 0; i < need; i++) {
                    node.addRecord(next.getKey(i), next.getRecord(i));
                    next.deleteRecord(i);
                }
            }

            else {
                for (int i = 0; i < need; i++) {
                    node.addRecord(prev.getKey(prev.getKeys().size()-1 -i), prev.getRecord(prev.getKeys().size()-1 -i));
                    prev.deleteRecord(prev.getKeys().size()-1 -i);
                }
            }
            
            temp = node.getParent();
        }
        resetParent(temp);
    }

    //update parents
    public void resetParent(ParentNode parent) {
        if (parent.getIsRoot()) {
            if (parent.getChildren().size() > 1) {
                Node child = parent.getChild(0);
                parent.deleteChild(child);
                parent.addChild(child);
                return;
            }
            //only has one child
            else {
                parent.getChild(0).setIsRoot(true);
                root = parent.getChild(0);
                parent.deleteNode();
                nodeDeleted++;
                height--;
                return;
            }
        }

        ParentNode prev = (ParentNode) parent.getParent().getPrev(parent);
        ParentNode next = (ParentNode) parent.getParent().getAfter(parent);
        int need = parentMinKeys - parent.getKeys().size();
        int prevAvail = 0;
        int nextAvail = 0;
        ParentNode temp;

        if (prev != null) 
            prevAvail += prev.getKeys().size() - parentMinKeys;
        if (next != null) 
            nextAvail += next.getKeys().size() - parentMinKeys;


        if (need > nextAvail + prevAvail) {
            if (prev != null && next != null) {
                for (int i = 0; i < n-(prevAvail+parentMinKeys)+1 && i < parent.getChildren().size(); i++) 
                    prev.addChild(parent.getChild(i));
                for (int i = n-(prevAvail+parentMinKeys)+1; i < parent.getChildren().size(); i++) 
                    next.addChild(parent.getChild(i));
            }

            else if (prev == null) {
                for (int i = 0; i < parent.getChildren().size(); i++) 
                    next.addChild(parent.getChild(i));
            }
            else {
                for (int i = 0; i < parent.getChildren().size(); i++) 
                    prev.addChild(parent.getChild(i));
            }

            temp = parent.getParent();
            parent.deleteNode();
            nodeDeleted++;
        }

        else {
            if (prev != null && next != null) {
                for (int i = 0; i < prevAvail && i < need; i++) {
                    parent.addChild(prev.getChild(prev.getChildren().size()-1), 0);
                    prev.deleteChild(prev.getChild(prev.getChildren().size()-1));
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
            }
            else {
                for (int i = 0; i < need; i++) {
                    parent.addChild(prev.getChild(prev.getChildren().size()-1 -i), 0);
                    prev.deleteChild(prev.getChild(prev.getChildren().size()-1 -i));
                }
            } 
            temp = parent.getParent();
        }
        resetParent(temp);
    }

    public ArrayList<Address> getRecordsWithKey(int key){
        ArrayList<Address> result = new ArrayList<>();
        int blockAccess = 1;
        int siblingAccess = 0;

        Log.d("key search: ","[Node Access] Access root node");

        Node curr = root;
        ParentNode parentNode;
   
        //search and stop before leaf node
        while (!curr.getIsLeaf()) {
            parentNode = (ParentNode) curr;
            for (int i=0; i<parentNode.getKeys().size(); i++) {
                if ( key <= parentNode.getKey(i)){
                    Log.v("key search: ", curr.toString());
                    Log.d("key search: ",String.format("[Node Access] follow pointer [%d]: key(%d)<=curKey(%d)", i, key, parentNode.getKey(i) ));
                    curr = parentNode.getChild(i);
                    blockAccess++;
                    break;
                }
                if (i == parentNode.getKeys().size()-1){
                    Log.v("key search: ", curr.toString());
                    Log.d("key search: ",String.format("[Node Access] follow pointer [%d+1]: last key and key(%d)>curKey(%d)", i, key, parentNode.getKey(i) ));
                    curr = parentNode.getChild(i+1);
                    blockAccess++;
                    break;
                }
            }
        }
        
        LeafNode curLeaf = (LeafNode) curr;
        boolean finished = false;

        while(!finished && curLeaf!=null){
            for (int i=0; i<curLeaf.getKeys().size(); i++){
                //find same key
                if (curLeaf.getKey(i) == key){
                    result.add(curLeaf.getRecord(i));
                    continue;
                }
                if (curLeaf.getKey(i) > key){
                    finished = true;
                    break;
                }
            }
            //check next node
            if (!finished){
                if (curLeaf.getNext()!= null){
                    curLeaf = curLeaf.getNext();
                    blockAccess++;
                    siblingAccess++;
                } else {
                    break;
                }
            }
        }

        if (siblingAccess > 0){
            Log.d("key search", "[Node Access] " + siblingAccess + " sibling node access");
        }
    
        Log.i("key search", String.format("input(%d): %d records found with %d node access", key, result.size(), blockAccess));

        return result;
    }


    //didn't change this function
    public void treeStats() {

        ArrayList<Integer> rootKeys = new ArrayList<Integer>();
        ArrayList<Integer> firstKeys = new ArrayList<Integer>();
        ParentNode rootCopy = (ParentNode) root;
        Node first = rootCopy.getChild(0);

        for (int i = 0; i < root.getKeys().size(); i++) {

            rootKeys.add(root.getKey(i));
        }

        for (int i = 0; i < first.getKeys().size(); i++) {

            firstKeys.add(first.getKey(i));
        }

        Log.d("treeStats", "n = " + n + ", number of nodes = " + noOfNode + ", height = " + height);
        Log.d("rootContents", "root node contents = " + rootKeys);
        Log.d("firstContents", "first child contents = " + firstKeys);
    }

    // used in main
    // TODO for Experiment 4
    public ArrayList<Address> getRecordsWithKeyInRange(int min, int max) {
        return getRecordsWithKeyInRange(min, max, true);
    }

    // changed: curNode to curr
    public ArrayList<Address> getRecordsWithKeyInRange(int min, int max, boolean isVerbose){
        ArrayList<Address> result = new ArrayList<>();
        int nodeAccess = 1; // access the root??
        int siblingAccess = 0;
        if (isVerbose) {
            Log.d("B+Tree.rangeSearch", "[Node Access] Access root node");
        }
        Node curr = root;
        ParentNode parentNode;
        // searching for leaf node with key
        while (!curr.getIsLeaf()){
            parentNode = (ParentNode) curr;
            for (int i=0; i<parentNode.getKeys().size(); i++) {
                if ( min <= parentNode.getKey(i)){
                    if (isVerbose) {
                        Log.v("B+Tree.rangeSearch", curr.toString());
                        Log.d("B+Tree.rangeSearch", String.format("[Node Access] follow pointer [%d]: min(%d)<=curKey(%d)", i, min, parentNode.getKey(i)));
                    }
                    curr = parentNode.getChild(i);
                    nodeAccess++;
                    break;
                }
                if (i == parentNode.getKeys().size()-1){
                    if (isVerbose) {
                        Log.v("B+Tree.rangeSearch", curr.toString());
                        Log.d("B+Tree.rangeSearch", String.format("[Node Access] follow pointer [%d+1]: last key and min(%d)>curKey(%d)", i, min, parentNode.getKey(i)));
                    }
                    curr = parentNode.getChild(i+1);
                    nodeAccess++;
                    break;
                }
            }
        }
        // next leaf node is found, find all records with same key
        LeafNode curLeaf = (LeafNode) curr;
        boolean finished = false;
        while(!finished && curLeaf!=null){
            // finding same keys within leaf node
            for (int i=0; i<curLeaf.getKeys().size(); i++){
                // found same key, add into result list
                if (curLeaf.getKey(i) >= min && curLeaf.getKey(i) <= max){
                    result.add(curLeaf.getRecord(i));
                    continue;
                }
                // if curKey > searching key, no need to continue searching
                if (curLeaf.getKey(i) > max){
                    finished = true;
                    break;
                }
            }
            if (!finished){
                // trying to check sibling node has remaining records of same key
                if (curLeaf.getNext()!= null){
                    curLeaf = (LeafNode) curLeaf.getNext();
                    nodeAccess++;
                    siblingAccess++;
                } else {
                    break;
                }
            }
        }
        if (siblingAccess > 0){
            if (isVerbose) {
                Log.d("B+Tree.rangeSearch", "[Node Access] " + siblingAccess + " sibling node access");
            }
        }
        if (isVerbose) {
            Log.i("B+Tree.rangeSearch", String.format("input(%d, %d): %d records found with %d node access", min, max, result.size(), nodeAccess));
        }
        return result;
    }

    // TODO for Experiment 5
    public ArrayList<Address> removeRecordsWithKey(){
        // list of address need to be return, so app can use it to delete records from disk
        return null;
    }

    public void logStructure(){
        logStructure(0, Integer.MAX_VALUE, root);
    }

    public void logStructure(int maxLevel){
        logStructure(0, maxLevel, root);
    }

    // changed: curNode to curr
    // recursive logging of tree structure
    private void logStructure(int level, int maxLevel,  Node curr){
        if (curr == null){
            curr = root;
        }
        if (level > maxLevel){
            return;
        }

        System.out.print("h="+level+"; ");
        curr.logStructure();
        if (curr.getIsLeaf()){
            return;
        }
        ParentNode parentNode = (ParentNode) curr;
        for (Node child: parentNode.getChildren()) {
            logStructure(level+1, maxLevel, child);
        }
    }
}

/* main changes:
 * deleted createFirst, put the implementation into BPlusTree
 * deleted insertLeafNode, put into implementation of insert
 * split getRecordsWithKey to 1 getRecordsWithKey and 1 findRecordsWithKey
 * move the first if statement (no need to cheng node) in resetLeaf to delete key insyead
 * deleted splitLeaf, put into insert
 */