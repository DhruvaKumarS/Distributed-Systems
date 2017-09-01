package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

/**
 * Created by dhruv on 24-Apr-17.
 */
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class Node {
    String nodeID;
    String predecessor;
    String predecessor2;
    String successor;
    String successor2;
    static String TAG = "NodeClass";
    @Override
    public String toString() {
        return "Node{" +
                "nodeID='" + nodeID + '\'' +
                ", predecessor='" + predecessor + '\'' +
                ", predecessor2='" + predecessor2 + '\'' +
                ", successor='" + successor + '\'' +
                ", successor2='" + successor2 + '\'' +
                '}';
    }

    public String getHashedNodeID() {
        try {
            return genHash(nodeID);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }
        return null;
    }

    public String getHashedPredecessor() {
        try {
            return genHash(predecessor);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }
        return null;
    }

    public String getHashedSuccessor() {
        try {
            return genHash(successor);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }
        return null;
    }

    public String getHashedPredecessor2() {
        try {
            return genHash(predecessor2);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }
        return null;
    }

    public String getHashedSuccessor2() {
        try {
            return genHash(successor2);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }
        return null;
    }

    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}