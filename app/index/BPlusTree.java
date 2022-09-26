package app.index;

import app.storage.Address;
import app.util.Log;
import java.util.ArrayList;

public class BPlusTree {
    private static final String TAG = "B+Tree";
    private static final int SIZE_POINTER = 6; //for 64 bits system
    private static final int SIZE_KEY = 4; //for int
    int n;
    int parentMinKeys;
    int leafMinKeys;
    Node root;
    int height;
    int noOfNode;
    int nodeDeleted;

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
            Address addresses[] = new Address[n+1];
            LeafNode newLeaf = new LeafNode();
    
            //getting full list
            for (int i = 0; i < n; i++) {
                keys[i] = found.getKey(i);
                addresses[i] = found.getRecord(i);
            }
        
            // sort
            for (int i = n-1; i >= 0; i--) {
                 if (keys[i] <= key) {
                    i++;
                    keys[i] = key;
                     addresses[i] = address;
                     break;
                }
                keys[i+1] = keys[i];
                addresses[i+1] = addresses[i];
            } 
    
            //clearing found leafnode values
            found.preSplit();
    
            //putting the keys and addresses into the two leafnodes
            for (int i = 0; i < leafMinKeys; i++) 
                found.addRecord(keys[i], addresses[i]);
    
            for (int i = leafMinKeys; i < n+1; i++) 
                newLeaf.addRecord(keys[i], addresses[i]);
    
            //setting found leafnode to point to new leafnode and new leafnode to point to next leafnode
            newLeaf.setNext(found.getNext());
            found.setNext(newLeaf);
    
            //setting parents for new leafnode
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

    //to split a full parent node
    public void splitParent(ParentNode parent, Node child) {
        Node children[] = new Node[n+2];
        int keys[] = new int[n+2];
        int key = child.findSmallestKey();
        ParentNode newParent = new ParentNode();

        // getting full and sorted lists of keys and children
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

        //clearing old parent values
        parent.preSplit();

        // putting the children into the two parentnodes
        for (int i = 0; i < parentMinKeys+2; i++) 
            parent.addChild(children[i]);

        for (int i = parentMinKeys+2; i < n+2; i++) 
            newParent.addChild(children[i]);

        //setting parent for the new parentnode
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

        // updating noOfNode
        noOfNode++;
    }

    // used in main
    // to delete all records of a certain key
    public void deleteKey(int key) {
        ArrayList<Integer> keys;
        LeafNode leaf;

        // while there are still records with given key value
        while (findRecordsWithKey(key).size() != 0) {

            leaf = findLeaf(key);
            keys = leaf.getKeys();
            
            // delete one record and update tree 
            for (int i = 0; i < keys.size(); i++) {
                
                if (keys.get(i) == key) {

                    leaf.deleteRecord(i);

                    // if leafnode is not root then update tree
                    if (!leaf.getIsRoot())
                        resetLeaf(leaf);

                    break;
                }
            }
        }

        Log.d("deletion", "number of nodes deleted = " + nodeDeleted);
        noOfNode -= nodeDeleted;
        treeStats();
    }

    public ArrayList<Address> findRecordsWithKey(int key) {
        ArrayList<Address> result = new ArrayList<>();
        Node curr = root;
        ParentNode parent;
        // searching for leaf node with key
        while (!curr.getIsLeaf()){
            parent = (ParentNode) curr;
            for (int i=0; i<parent.getKeys().size(); i++) {
                if ( key <= parent.getKey(i)){
                    curr = parent.getChild(i);
                    break;
                }
                if (i == parent.getKeys().size()-1){
                    curr = parent.getChild(i+1);
                    break;
                }
            }
        }
        // after leaf node is found, find all records with same key
        LeafNode curLeaf = (LeafNode) curr;
        boolean done = false;
        while(!done && curLeaf != null){
            // finding same keys within leaf node
            for (int i=0; i<curLeaf.getKeys().size(); i++){
                // found same key, add into result list
                if (curLeaf.getKey(i) == key){
                    result.add(curLeaf.getRecord(i));
                    continue;
                }
                // if curKey > searching key, no need to continue searching
                if (curLeaf.getKey(i) > key){
                    done = true;
                    break;
                }
            }
            if (!done){
                // trying to check sibling node has remaining records of same key
                if (curLeaf.getNext()!= null){
                    curLeaf = curLeaf.getNext();
                } else {
                    break;
                }
            }
        }
        return result;
    }

    // to update leafnode
    public void resetLeaf(LeafNode node) {
        
        int needed = leafMinKeys - node.getKeys().size();
        LeafNode before = (LeafNode) node.getParent().getBefore(node);
        LeafNode after = (LeafNode) node.getParent().getAfter(node);
        int b = 0;
        int a = 0;
        ParentNode copy;

        // getting number of keys that before and after nodes can spare
        if (before != null) 
            b += before.getKeys().size() - leafMinKeys;

        if (after != null) 
            a += after.getKeys().size() - leafMinKeys;

        // if need to merge
        if (needed > a + b) {

            // if node has both before and after nodes
            if (before != null && after != null) {

                // insert as many records as possible into before node
                for (int i = 0; i < n-(b+leafMinKeys); i++) 
                    before.addRecord(node.getKey(i), node.getRecord(i));
                
                // insert the rest into after node
                for (int i = n-(b+leafMinKeys); i < node.getKeys().size(); i++) 
                    after.addRecord(node.getKey(i), node.getRecord(i));
            }

            // if node only has after node
            else if (before == null) {

                for (int i = 0; i < node.getKeys().size(); i++) 
                    after.addRecord(node.getKey(i), node.getRecord(i));
            }

            // if node only has before node
            else {

                for (int i = 0; i < node.getKeys().size(); i++) 
                    before.addRecord(node.getKey(i), node.getRecord(i));
            }

            // have to copy parent to reset after deleting leafnode
            copy = node.getParent();

            // have to look for before node if it is not from the same parent
            if (before == null) {

                if (!copy.getIsRoot())
                    before = findLeaf(copy.findSmallestKey()-1);
            }

            // change before to point to after
            before.setNext(node.getNext());

            // delete node
            node.deleteNode();
            nodeDeleted++;
        }

        // if able to borrow keys
        else {

            if (before != null && after != null) {

                // take the last few keys from before node that can be spared
                for (int i = 0; i < b; i++) {

                    node.addRecord(before.getKey(before.getKeys().size()-1 -i), before.getRecord(before.getKeys().size()-1 -i));
                    before.deleteRecord(before.getKeys().size()-1 -i);
                }
                
                // take the rest from after node
                for (int i = b, j = 0; i < needed; i++, j++) {

                    node.addRecord(after.getKey(j), after.getRecord(j));
                    after.deleteRecord(j);
                }
            }

            else if (before == null) {

                // take all from after node
                for (int i = 0; i < needed; i++) {

                    node.addRecord(after.getKey(i), after.getRecord(i));
                    after.deleteRecord(i);
                }
            }

            else {

                // take all from before node
                for (int i = 0; i < needed; i++) {

                    node.addRecord(before.getKey(before.getKeys().size()-1 -i), before.getRecord(before.getKeys().size()-1 -i));
                    before.deleteRecord(before.getKeys().size()-1 -i);
                }
            }
            
            copy = node.getParent();
        }

        // update parents
        resetParent(copy);
    }

    public void resetParent(ParentNode parent) {

        // if node is root
        if (parent.getIsRoot()) {

            // if root has at least 2 children, reset and return
            if (parent.getChildren().size() > 1) {

                // lazy man's reset
                Node child = parent.getChild(0);
                parent.deleteChild(child);
                parent.addChild(child);
                return;
            }

            // if root has 1 child, delete root level
            else {

                parent.getChild(0).setIsRoot(true);
                root = parent.getChild(0);
                parent.deleteNode();
                nodeDeleted++;
                height--;
                return;
            }
        }

        ParentNode before = (ParentNode) parent.getParent().getBefore(parent);
        ParentNode after = (ParentNode) parent.getParent().getAfter(parent);
        int needed = parentMinKeys - parent.getKeys().size();
        int b = 0;
        int a = 0;
        ParentNode copy;

        if (before != null) 
            b += before.getKeys().size() - parentMinKeys;

        if (after != null) 
            a += after.getKeys().size() - parentMinKeys;

        // if need to merge
        if (needed > a + b) {

            // if node has both before and after nodes
            if (before != null && after != null) {

                // insert as many records as possible into before node
                for (int i = 0; i < n-(b+parentMinKeys)+1 && i < parent.getChildren().size(); i++) 
                    before.addChild(parent.getChild(i));
                
                // insert the rest into after node
                for (int i = n-(b+parentMinKeys)+1; i < parent.getChildren().size(); i++) 
                    after.addChild(parent.getChild(i));
            }

            // if node only has after node
            else if (before == null) {

                for (int i = 0; i < parent.getChildren().size(); i++) 
                    after.addChild(parent.getChild(i));
            }

            // if node only has before node
            else {

                for (int i = 0; i < parent.getChildren().size(); i++) 
                    before.addChild(parent.getChild(i));
            }

            // delete after merging
            copy = parent.getParent();
            parent.deleteNode();
            nodeDeleted++;
        }

        // if able to borrow keys
        else {
            if (before != null && after != null) {

                // take the last few keys from before node that can be spared
                for (int i = 0; i < b && i < needed; i++) {

                    parent.addChild(before.getChild(before.getChildren().size()-1), 0);
                    before.deleteChild(before.getChild(before.getChildren().size()-1));
                }
                
                // take the rest from after node
                for (int i = b; i < needed; i++) {

                    parent.addChild(after.getChild(0));
                    after.deleteChild(after.getChild(0));
                }
            }

            else if (before == null) {

                // take all from after node
                for (int i = 0; i < needed; i++) {

                    parent.addChild(after.getChild(0));
                    after.deleteChild(after.getChild(0));
                }
            }

            else {

                // take all from before node
                for (int i = 0; i < needed; i++) {

                    parent.addChild(before.getChild(before.getChildren().size()-1 -i), 0);
                    before.deleteChild(before.getChild(before.getChildren().size()-1 -i));
                }
            }
            
            copy = parent.getParent();
        }

        resetParent(copy);
    }

    public ArrayList<Address> getRecordsWithKey(int key){
        ArrayList<Address> result = new ArrayList<>();
        int blockAccess = 1; // access the root??
        int siblingAccess = 0;
        Log.d("B+Tree.keySearch","[Node Access] Access root node");
        Node curr = root;
        ParentNode parentNode;
        // searching for leaf node with key
        while (!curr.getIsLeaf()){
            parentNode = (ParentNode) curr;
            for (int i=0; i<parentNode.getKeys().size(); i++) {
                if ( key <= parentNode.getKey(i)){
                    Log.v("B+Tree.keySearch", curr.toString());
                    Log.d("B+Tree.keySearch",String.format("[Node Access] follow pointer [%d]: key(%d)<=curKey(%d)", i, key, parentNode.getKey(i) ));
                    curr = parentNode.getChild(i);
                    blockAccess++;
                    break;
                }
                if (i == parentNode.getKeys().size()-1){
                    Log.v("B+Tree.keySearch", curr.toString());
                    Log.d("B+Tree.keySearch",String.format("[Node Access] follow pointer [%d+1]: last key and key(%d)>curKey(%d)", i, key, parentNode.getKey(i) ));
                    curr = parentNode.getChild(i+1);
                    blockAccess++;
                    break;
                }
            }
        }
        // after leaf node is found, find all records with same key
        LeafNode curLeaf = (LeafNode) curr;
        boolean done = false;
        while(!done && curLeaf!=null){
            // finding same keys within leaf node
            for (int i=0; i<curLeaf.getKeys().size(); i++){
                // found same key, add into result list
                if (curLeaf.getKey(i) == key){
                    result.add(curLeaf.getRecord(i));
                    continue;
                }
                // if curKey > searching key, no need to continue searching
                if (curLeaf.getKey(i) > key){
                    done = true;
                    break;
                }
            }
            if (!done){
                // trying to check sibling node has remaining records of same key
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
            Log.d("B+Tree.keySearch", "[Node Access] " + siblingAccess + " sibling node access");
        }
    
        Log.i("B+Tree.keySearch", String.format("input(%d): %d records found with %d node access", key, result.size(), blockAccess));

        return result;
    }

    // used in main
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
        // after leaf node is found, find all records with same key
        LeafNode curLeaf = (LeafNode) curr;
        boolean done = false;
        while(!done && curLeaf!=null){
            // finding same keys within leaf node
            for (int i=0; i<curLeaf.getKeys().size(); i++){
                // found same key, add into result list
                if (curLeaf.getKey(i) >= min && curLeaf.getKey(i) <= max){
                    result.add(curLeaf.getRecord(i));
                    continue;
                }
                // if curKey > searching key, no need to continue searching
                if (curLeaf.getKey(i) > max){
                    done = true;
                    break;
                }
            }
            if (!done){
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

/* changes made:
 * added found var to insert
 * leaf2 to newLeaf in split
 * curNode to curr for all functions
 * nodeCount to noOfNode
 * deletedCount to nodeDeleted
 */