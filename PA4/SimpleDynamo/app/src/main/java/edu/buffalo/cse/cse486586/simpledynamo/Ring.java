package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by dhruv on 24-Apr-17.
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.PriorityQueue;
import android.util.Log;

public class Ring {
    static final String TAG = "RingClass";
    ArrayList<Node> nodes;
    public Ring(PriorityQueue<String> nodeIds, HashMap<String, String> nodeHashMap){
        nodes = new ArrayList<Node>();
        ArrayList<String> nodeIdsArr = new ArrayList<String>(nodeIds);
        int ctr = 0;
        int nodeCount = nodeIdsArr.size();
        //Log.i(TAG, Arrays.toString(nodeIdsArr.toArray()));
        while(ctr < nodeCount){
            Node node = new Node();
            node.nodeID = nodeHashMap.get(nodeIdsArr.get(ctr));
            node.predecessor = nodeHashMap.get(nodeIdsArr.get(ctr - 1 < 0 ? ctr - 1 + nodeCount : ctr - 1));
            node.predecessor2 = nodeHashMap.get(nodeIdsArr.get(ctr - 2 < 0 ? ctr - 2 + nodeCount : ctr - 2));
            node.successor = nodeHashMap.get(nodeIdsArr.get(ctr + 1 >= nodeCount ? (ctr + 1) % nodeCount : ctr + 1));
            node.successor2 = nodeHashMap.get(nodeIdsArr.get(ctr + 2 >= nodeCount ? (ctr + 2) % nodeCount : ctr + 2));
            nodes.add(node);
            ctr++;
            //Log.i(TAG, "Adding "+node.nodeID);
        }
    }

    public Node getCurrentNode(String currentPort){
        int ctr = 0;
        int nodeCount = nodes.size();
        while(ctr < nodeCount){
            if(nodes.get(ctr).nodeID.compareTo(currentPort) == 0)
                return nodes.get(ctr);
            ctr++;
        }
        return null;
    }

    public Node getDestinationNode(String hashedKey){
        int ctr = 0;
        int nodeCount = nodes.size();

        while(ctr < nodeCount && ctr + 1 < nodeCount){
            if(hashedKey.compareTo(nodes.get(ctr).getHashedNodeID()) > 0
                    && hashedKey.compareTo(nodes.get(ctr + 1).getHashedNodeID()) <= 0)
                return nodes.get(ctr + 1);
            ctr++;
        }
        return nodes.get(0);
    }

    public ArrayList<String> getOtherNodesList(Node currentNode){
        ArrayList<String> otherNodesList = new ArrayList<String>();
        int ctr = 0;
        int nodeCount = nodes.size();

        while(ctr < nodeCount){
            if(currentNode.nodeID.compareTo(nodes.get(ctr).nodeID) != 0)
                otherNodesList.add(nodes.get(ctr).nodeID);
            ctr++;
        }
        return otherNodesList;
    }
}
