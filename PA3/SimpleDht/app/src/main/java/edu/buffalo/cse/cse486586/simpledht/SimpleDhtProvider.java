package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String GLOBAL_DHT = "*";
    private static final String LOCAL_DHT = "@";
    static final String TAG = "SimpleDhtProvider";
    static final String EMPTY_STRING = "";
    static final String NEW_LINE = "\n";
    static final String ACKNOWLEDGE_MESSAGE = "ACK_MSG";

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    static final String[] columnNames = {KEY_FIELD, VALUE_FIELD};
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    static final int SERVER_PORT = 10000;

    static String PREDECESSOR = "";
    static String SUCCESSOR = "";
    static final String PORT0 = "5554";
    static final String PORT1 = "5556";
    static final String PORT2 = "5558";
    static final String PORT3 = "5560";
    static final String PORT4 = "5562";
    static final String[] remotePortList = {PORT0, PORT1, PORT2, PORT3, PORT4};
    static String CURRENT_PORT;
    static int NODE_COUNT = 1;
    static final String MAX_HASH_VALUE = "fffffffffffffffffffffffffffffffffffffffe";
    static String MAX_NODE_ID;
    static ArrayList<String> keyInsertedList = new ArrayList<String>();
    static PriorityQueue<String> nodeIDs = new PriorityQueue<String>();
    static HashMap<String, String> nodePortMap = new HashMap<String, String>();
    static final String GET_NODE = "GET_NODE";
    static final String SEND_NODE = "SEND_NODE";

    private class Node{
        String nodeID;
        String predecessor;
        String successor;

        @Override
        public String toString() {
            return "Node{" +
                    "nodeID='" + nodeID + '\'' +
                    ", predecessor='" + predecessor + '\'' +
                    ", successor='" + successor + '\'' +
                    '}';
        }

        public String getHashedNodeID(){
            try{
                return genHash(nodeID);
            } catch (Exception e){
                Log.e(TAG, e.toString());
            }
            return null;
        }

        public String getHashedPredecessor(){
            try{
                return genHash(predecessor);
            } catch (Exception e){
                Log.e(TAG, e.toString());
            }
            return null;
        }

        public String getHashedSuccessor(){
            try{
                return genHash(successor);
            } catch (Exception e){
                Log.e(TAG, e.toString());
            }
            return null;
        }

        /*private String findSuccessor(String id){
            Node n = findPredecessor(id);
            Log.i(TAG,"Successor: "+n.successor);
            return n.successor;
        }*/
        private boolean checkCondition(String id, Node n){
            //Log.i("checkCondition", "MAX_NODE_ID: "+MAX_NODE_ID);
            BigInteger idVal = getBI(id);
            BigInteger maxNodeVal = getBI(MAX_NODE_ID);
            BigInteger nodeVal = getBI(n.getHashedNodeID());
            BigInteger successorVal = getBI(n.getHashedSuccessor());
            BigInteger predecessorVal = getBI(n.getHashedPredecessor());
            /*if((node.getHashedNodeID().compareTo(node.getHashedSuccessor()) < 0
                    && id.compareTo(node.getHashedNodeID()) > 0
                    && id.compareTo(node.getHashedSuccessor()) <= 0)
                    || (node.getHashedNodeID().compareTo(node.getHashedSuccessor()) > 0
                    && ((id.compareTo(node.getHashedSuccessor()) > 0
                    && id.compareTo(node.getHashedNodeID()) > 0)
                    || (id.compareTo(node.getHashedSuccessor()) < 0
                    && id.compareTo(node.getHashedNodeID()) < 0)))
                    || (node.getHashedNodeID().compareTo(node.getHashedPredecessor()) == 0
                    && node.getHashedNodeID().compareTo(node.getHashedSuccessor()) == 0)){
                return true;
            }*/
            if(idVal.compareTo(nodeVal) > 0 && idVal.compareTo(successorVal) <= 0) {
                return true;
            }
            else if(idVal.compareTo(nodeVal) > 0 && idVal.compareTo(successorVal) > 0
                    && nodeVal.compareTo(successorVal) > 0) {
                return true;
            }
            else if(idVal.compareTo(nodeVal) < 0 && idVal.compareTo(successorVal) < 0
                    && nodeVal.compareTo(successorVal) > 0){
                return true;
            }
            else if(nodeVal.compareTo(maxNodeVal) == 0 && idVal.compareTo(nodeVal) < 0
                    && idVal.compareTo(successorVal) < 0){
                return true;
            }
            else {
                return false;
            }
        }

        private Node findPredecessor(String id){
            Node n = this;
            int ctr = 0;
            Log.i(TAG, n.toString());
            Log.i(TAG, "Get successor for: "+id);
            try {
                while(!checkCondition(genHash(id), n)){
                    Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(n.successor)*2);
                    Log.i(TAG, "Getting node : " + n.successor);
                    //String message = GET_NODE;
                    PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
                    BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    msgSend.println(GET_NODE);
                    while (!client.isClosed()) {
                        String ack = ackGet.readLine();
                        Log.i(TAG, ack);
                        if(ack.contains(SEND_NODE)){
                            Log.i("TAG","Received ack: "+ack);
                            n = new Node();
                            String[] rcvNode = ack.split(":");
                            n.nodeID = rcvNode[1];
                            n.predecessor = rcvNode[2];
                            n.successor = rcvNode[3];

                            client.close();
                        }
                    }
                    msgSend.close();
                    ackGet.close();

                    ctr += 1;
                    if (client.isClosed()) {
                        Log.i(TAG, "Received ACK and closed socket");
                    } else {
                        Log.i(TAG, "Socket not closed. Verify logic");
                    }
                }
                Log.i("findPredecessor", n.toString());
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            /*if(ctr == 0){
                n.successor = id;
            }*/
            return n;
        }
    }
    static Node node;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        //FileInputStream inputStream;
        //String[] columnNames = {KEY_FIELD, VALUE_FIELD};
        //String fileName;
        String fileContent;
        int count = 0;
        if(!selection.contains("PORT_")){
            selection = "PORT_" + CURRENT_PORT + "&" + selection;
        }
        //byte[] content = new byte[50];
        //MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        ArrayList<String> filesToDelete = new ArrayList<String>();
        try {
            //fileName = KEY_FIELD + Integer.toString(i);
            //Log.i(TAG, selection);
            Log.i("delete", selection.split("&")[0]);
            if(selection.split("&")[1].equals(GLOBAL_DHT)){
                filesToDelete.addAll(keyInsertedList);
                if(node.getHashedNodeID().compareTo(node.getHashedSuccessor()) == 0
                        || selection.contains(node.successor)) {
                    for(String fileName : filesToDelete){
                        //Log.i(TAG, "Reading file: "+fileName);
                        getContext().deleteFile(fileName);
                        count += 1;
                    }

                } else {
                    count = deleteKeys(selection, node);
                    for(String fileName : filesToDelete){
                        //Log.i(TAG, "Reading file: "+fileName);
                        getContext().deleteFile(fileName);
                        count += 1;
                    }

                    Log.i("delete","Delete: "+count);
                }
            }
            else if(selection.split("&")[1].equals(LOCAL_DHT)){
                filesToDelete.addAll(keyInsertedList);
                for(String fileName : filesToDelete){
                    //Log.i(TAG, "Reading file: "+fileName);
                    getContext().deleteFile(fileName);
                    count += 1;
                }
            }
            else{
                Log.i("delete", selection.split("&")[1]);
                String hashFileName = genHash(selection.split("&")[1]);
                if((node.getHashedNodeID().compareTo(node.getHashedPredecessor()) > 0
                        && hashFileName.compareTo(node.getHashedPredecessor()) > 0
                        && hashFileName.compareTo(node.getHashedNodeID()) <= 0)
                        || (node.getHashedNodeID().compareTo(node.getHashedPredecessor()) < 0
                        && ((hashFileName.compareTo(node.getHashedPredecessor()) > 0
                        && hashFileName.compareTo(node.getHashedNodeID()) > 0)
                        || (hashFileName.compareTo(node.getHashedPredecessor()) < 0
                        && hashFileName.compareTo(node.getHashedNodeID()) <= 0)))
                        || (node.getHashedNodeID().compareTo(node.getHashedPredecessor()) == 0
                        && node.getHashedNodeID().compareTo(node.getHashedSuccessor()) == 0)){
                    filesToDelete.add(selection.split("&")[1]);
                    for(String fileName : filesToDelete){
                        //Log.i(TAG, "Reading file: "+fileName);
                        getContext().deleteFile(fileName);
                        count += 1;
                    }
                    Log.i("delete", "Deleted: "+count);
                } else {
                    count = deleteKeys(selection, node);
                }

            }

            /*if(selection.trim().equals(GLOBAL_DHT)){
                filesToDelete.addAll(keyInsertedList);
            }
            else if(selection.trim().equals(LOCAL_DHT)){
                filesToDelete.addAll(keyInsertedList);
            }
            else{
                filesToDelete.add(selection);
            }
            for(String fileName : filesToDelete){
                //Log.i(TAG, "Reading file: "+fileName);
                getContext().deleteFile(fileName);
                count += 1;
            }*/
        }
        catch (Exception e){
            Log.e(TAG, e.toString());
        }
        //Log.i(TAG, Integer.toString(matrixCursor.getCount()));
        Log.v("delete", String.valueOf(count));
        return count;
    }

    private int deleteKeys(String selection, Node node) throws Exception {
        Log.i("deleteKeys", "Deleting "+selection+" in "+node.successor);
        Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(node.successor) * 2);
        int count = 0;
        //Log.i(TAG, "Client at port : " + params[0]);
        String message = "DELETE:" + selection;
        PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
        BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
        msgSend.println(message);
        String contents = "";
        while (!client.isClosed()) {
            String ack = ackGet.readLine();
            Log.i(TAG, ack);
            if (ack.contains(ACKNOWLEDGE_MESSAGE + "_DELETED")) {
                if(ack.length() > (ACKNOWLEDGE_MESSAGE + "_DELETED::").length()){
                    contents = ack.split("::")[1];
                } else {
                    contents = "";
                }

                client.close();
            }
        }
        Log.i("deleteKeys", contents);
        if(contents != null && contents.length() > 0)
            count = Integer.parseInt(contents);
        return count;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String fileName = (String) values.get(KEY_FIELD);
        String fileContent = (String) values.get(VALUE_FIELD);
        //Log.i(TAG, "Key : "+fileName);
        //Log.i(TAG, "Value : "+fileContent);

        try {
            String hashFileName = genHash(fileName);
            Log.v("insert", "Comparing "+hashFileName+" With "+node.getHashedNodeID()+" and "+node.getHashedSuccessor());
            if((node.getHashedNodeID().compareTo(node.getHashedPredecessor()) > 0
                    && hashFileName.compareTo(node.getHashedPredecessor()) > 0
                    && hashFileName.compareTo(node.getHashedNodeID()) <= 0)
                    || (node.getHashedNodeID().compareTo(node.getHashedPredecessor()) < 0
                        && ((hashFileName.compareTo(node.getHashedPredecessor()) > 0
                            && hashFileName.compareTo(node.getHashedNodeID()) > 0)
                            || (hashFileName.compareTo(node.getHashedPredecessor()) < 0
                                && hashFileName.compareTo(node.getHashedNodeID()) <= 0)))
                    || (node.getHashedNodeID().compareTo(node.getHashedPredecessor()) == 0
                    && node.getHashedNodeID().compareTo(node.getHashedSuccessor()) == 0)){
                    /*|| (hashFileName.compareTo(node.getHashedNodeID()) < 0
                        && hashFileName.compareTo(node.getHashedSuccessor()) < 0
                        && node.getHashedNodeID().compareTo(node.getHashedSuccessor()) < 0
                        && node.getHashedNodeID().compareTo(node.getHashedPredecessor()) < 0)
                    || (hashFileName.compareTo(node.getHashedNodeID()) > 0
                        && hashFileName.compareTo(node.getHashedSuccessor()) > 0
                        && node.getHashedNodeID().compareTo(node.getHashedSuccessor()) < 0
                        && node.getHashedNodeID().compareTo(node.getHashedPredecessor()) < 0)*/

                FileOutputStream outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                outputStream.write(fileContent.getBytes());
                outputStream.close();
                keyInsertedList.add(fileName);
                Log.i("insert","Inserted in if: "+fileName);
            } else {
                Log.i(TAG, "Sending "+fileName+" to "+node.successor);
                sendKey(fileName, fileContent, node);
            }
            //Log.i(TAG, String.valueOf(keyInsertedList.size()));
        } catch (Exception e) {
            Log.e(TAG, "File write failed : " +fileName);
            e.printStackTrace();
        }
        Log.v("insert", values.toString());
        return uri;
    }

    private void sendKey(String fileName, String fileContent, Node node) throws Exception {
        Log.i("query", "Sending "+fileName+" to "+node.successor);
        Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(node.successor) * 2);
        //Log.i(TAG, "Client at port : " + params[0]);
        String message = "INSERT:" + fileName + ":" + fileContent;
        PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
        BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
        msgSend.println(message);
        String contents = "";
        while (!client.isClosed()) {
            String ack = ackGet.readLine();
            Log.i(TAG, ack);
            if (ack.contains(ACKNOWLEDGE_MESSAGE + "_INSERTED")) {
                client.close();
                return;
            }
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        if(!selection.contains("PORT_")){
            selection = "PORT_" + CURRENT_PORT + "&" + selection;
        }


        //String fileName;
        String fileContent;
        byte[] content = new byte[50];
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        ArrayList<String> filesToRead = new ArrayList<String>();
        try {
            //fileName = KEY_FIELD + Integer.toString(i);
            Log.i("query", selection.split("&")[0]);
            if(selection.split("&")[1].equals(GLOBAL_DHT)){
                filesToRead.addAll(keyInsertedList);
                if(node.getHashedNodeID().compareTo(node.getHashedSuccessor()) == 0
                        || selection.contains(node.successor)) {
                    matrixCursor = getKeys(filesToRead);

                } else {
                    matrixCursor = fetchKeys(selection, node, matrixCursor);
                    //if (selection.contains(node.nodeID)) {
                    Log.i("query", "From other nodes: "+ String.valueOf(matrixCursor.getCount()));
                    MatrixCursor currentMatrixCursor = new MatrixCursor(columnNames);
                    currentMatrixCursor = getKeys(filesToRead);
                    String keyValPairs = getKeyValFromCursor(currentMatrixCursor);
                    String[] keyValPairArr = keyValPairs.split(":");
                    int ctr = 0;
                    while (ctr + 1 < keyValPairArr.length) {
                        MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
                        //Log.i(TAG, "After");
                        mRowBuilder.add(columnNames[0], keyValPairArr[ctr]);
                        mRowBuilder.add(columnNames[1], keyValPairArr[ctr + 1]);
                        ctr += 2;
                    }
                    //}
                    Log.i("query", "From " + CURRENT_PORT + " and other nodes : "+ String.valueOf(matrixCursor.getCount()));
                    Log.i("query","Found: "+getKeyValFromCursor(matrixCursor));
                }
            }
            else if(selection.split("&")[1].equals(LOCAL_DHT)){
                filesToRead.addAll(keyInsertedList);
                matrixCursor = getKeys(filesToRead);
            }
            else{
                Log.i("query", selection.split("&")[1]);
                String hashFileName = genHash(selection.split("&")[1]);
                if((node.getHashedNodeID().compareTo(node.getHashedPredecessor()) > 0
                        && hashFileName.compareTo(node.getHashedPredecessor()) > 0
                        && hashFileName.compareTo(node.getHashedNodeID()) <= 0)
                        || (node.getHashedNodeID().compareTo(node.getHashedPredecessor()) < 0
                        && ((hashFileName.compareTo(node.getHashedPredecessor()) > 0
                        && hashFileName.compareTo(node.getHashedNodeID()) > 0)
                        || (hashFileName.compareTo(node.getHashedPredecessor()) < 0
                        && hashFileName.compareTo(node.getHashedNodeID()) <= 0)))
                        || (node.getHashedNodeID().compareTo(node.getHashedPredecessor()) == 0
                        && node.getHashedNodeID().compareTo(node.getHashedSuccessor()) == 0)){
                    filesToRead.add(selection.split("&")[1]);
                    matrixCursor = getKeys(filesToRead);
                    Log.i("query", "Found: "+matrixCursor.getCount());
                    Log.i("query", "Contents: "+getKeyValFromCursor(matrixCursor));
                } else {
                    matrixCursor = fetchKeys(selection, node, null);
                }

            }
        }
        catch (Exception e){
            Log.e(TAG, e.toString());
        }
        //Log.i(TAG, Integer.toString(matrixCursor.getCount()));
        Log.v("query", selection);
        return matrixCursor;
    }

    private MatrixCursor fetchKeys(String selection, Node node, MatrixCursor currentCursor) throws Exception {
        //String[] columnNames = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        String[] keyValPairs = null;


        Log.i("query", "Querying "+selection+" in "+node.successor);
        Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(node.successor) * 2);
                //Log.i(TAG, "Client at port : " + params[0]);
        String message = "QUERY:" + selection;
        PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
        BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
        msgSend.println(message);
        String contents = "";
        while (!client.isClosed()) {
            String ack = ackGet.readLine();
            Log.i(TAG, ack);
            if (ack.contains(ACKNOWLEDGE_MESSAGE + "_KEY_VALUE")) {
                if(ack.length() > (ACKNOWLEDGE_MESSAGE + "_KEY_VALUE::").length()){
                    contents = ack.split("::")[1];
                } else {
                    contents = "";
                }

                client.close();
            }
        }
        int ctr = 0;
        keyValPairs = contents.length() > 0 ? contents.split(":") : null;
        Log.i("fetchKeys", contents);
        while(keyValPairs != null && keyValPairs.length > 0 && ctr < keyValPairs.length) {
            MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
            mRowBuilder.add(columnNames[0], keyValPairs[ctr]);
            mRowBuilder.add(columnNames[1], keyValPairs[ctr + 1]);
            ctr += 2;
        }
        return matrixCursor;
    }

    private String getKeyValFromCursor(MatrixCursor matrixCursor) {
        //Log.i("getKeyValFromCursor","Here");
        String currentKeyValPairs = "";
        try {
            int keyIndex = matrixCursor.getColumnIndex(KEY_FIELD);
            int valueIndex = matrixCursor.getColumnIndex(VALUE_FIELD);
            matrixCursor.moveToFirst();
            if (matrixCursor.getCount() > 0) {
                do {
                    String returnKey = matrixCursor.getString(keyIndex);
                    String returnValue = matrixCursor.getString(valueIndex);
                    currentKeyValPairs = currentKeyValPairs + returnKey + ":" + returnValue + ":";
                    //Log.i("getKeyValFromCursor", currentKeyValPairs);
                } while (matrixCursor.moveToNext());

                matrixCursor.close();
                if (currentKeyValPairs.length() > 0) {
                    currentKeyValPairs = currentKeyValPairs.substring(0, currentKeyValPairs.length() - 1);
                }
            }
        } catch(Exception e){
            currentKeyValPairs = "";
            return currentKeyValPairs;
        }
        //matrixCursor.moveToFirst();
        return currentKeyValPairs;
    }

    private MatrixCursor getKeys(ArrayList<String> filesToRead){
        FileInputStream inputStream;
        //String[] columnNames = {KEY_FIELD, VALUE_FIELD};
        //String fileName;
        String fileContent;
        byte[] content = new byte[50];
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        for(String fileName : filesToRead){
            try {
                Log.i(TAG, "Reading file: " + fileName);
                inputStream = getContext().openFileInput(fileName);
                //inputStream.read();
                int length = inputStream.read(content);
                //Log.i(TAG, "content length : "+length);
                fileContent = new String(content).substring(0, length);
                Log.i(TAG, "fileContent : " + fileContent);
                MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
                //Log.i(TAG, "After");
                mRowBuilder.add(columnNames[0], fileName);
                mRowBuilder.add(columnNames[1], fileContent);
                //Log.i(TAG, "("+matrixCursor.getString(0)+":"+matrixCursor.getString(1)+")");
                inputStream.close();
            } catch (FileNotFoundException e){
                Log.e("getKeys", e.toString());
            } catch (IOException e){
                Log.e("getKeys", e.toString());
            }
        }
        return matrixCursor;
    }

    private BigInteger getBI(String port){
        BigInteger value = new BigInteger(port, 16);
        return value;
    }
    @Override
    public boolean onCreate() {

        TelephonyManager telManager = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portString = telManager.getLine1Number().substring(telManager.getLine1Number().length() - 4);
        final String portNumber = String.valueOf((Integer.parseInt(portString)) * 2);
        CURRENT_PORT = portString;
        node = new Node();
        node.nodeID = portString;
        node.successor = portString;
        node.predecessor = portString;
        try{
            //Log.i(TAG, Integer.parseInt(genHash(portNumber), 16));
            MAX_NODE_ID = genHash(PORT0);
            nodePortMap.put(genHash(PORT0), PORT0);
            nodePortMap.put(genHash(PORT1), PORT1);
            nodePortMap.put(genHash(PORT2), PORT2);
            nodePortMap.put(genHash(PORT3), PORT3);
            nodePortMap.put(genHash(PORT4), PORT4);
            /*for(int i = 0; i < 5; i++)
                getBI(genHash(remotePortList[i]));*/
            /*Iterator itr = nodePortMap.entrySet().iterator();
            String nodes = "";
            while(itr.hasNext()){
                nodes += itr.next() + "\n";
            }*/
            Log.i(TAG, "Nodes-Port hashmap: "+Arrays.asList(nodePortMap));
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if(!portNumber.equals(REMOTE_PORT0)){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REMOTE_PORT0, CURRENT_PORT);
            } else{
                nodeIDs.add(genHash(PORT0));
            }
        } catch (Exception e){
            Log.e(TAG, e.toString());
        }

        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try{
                while(true){
                    Socket client = serverSocket.accept();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter ackSend = new PrintWriter(client.getOutputStream(), true);
                    String msg;
                    Log.i("Server", "Connected" );
                    if((msg = reader.readLine()) != null) {
                        Log.i("Server", msg);
                        if (Arrays.asList(remotePortList).contains(msg) && msg.length() == 4) {
                            if (NODE_COUNT == 1 && node.nodeID.equals(PORT0)) {
                                node.successor = msg;
                                node.predecessor = msg;
                                MAX_NODE_ID = genHash(msg).compareTo(MAX_NODE_ID) > 0 ? genHash(msg) : MAX_NODE_ID;
                                //Log.i("Server task1", node.toString());
                                ackSend.println(ACKNOWLEDGE_MESSAGE + ":" + node.nodeID + ":" + node.nodeID);
                            } else {
                                Node insertLoc = node.findPredecessor(msg);

                                ackSend.println(ACKNOWLEDGE_MESSAGE + ":" + insertLoc.nodeID + ":" + insertLoc.successor);
                                MAX_NODE_ID = genHash(msg).compareTo(MAX_NODE_ID) > 0 ? genHash(msg) : MAX_NODE_ID;
                            }
                            //Log.i("onCreate", "MAX_NODE_ID: "+MAX_NODE_ID);
                            NODE_COUNT++;
                            nodeIDs.add(genHash(msg));
                        } else if (msg.contains(GET_NODE)) {
                            ackSend.println(SEND_NODE + ":" + node.nodeID + ":" + node.predecessor + ":" + node.successor);
                        } else if (msg.contains("SUCCESSOR") || msg.contains("PREDECESSOR")) {
                            Log.i("ACK", "Sending node ack");
                            if (msg.contains("SUCCESSOR")) {
                                node.successor = msg.split(":")[1];
                            } else {
                                node.predecessor = msg.split(":")[1];
                            }
                            ackSend.println(ACKNOWLEDGE_MESSAGE+"_NODE_MODIFIED");
                        } else if(msg.contains("INSERT")) {
                            String ackMsg = ACKNOWLEDGE_MESSAGE+"_INSERTED";

                            ContentValues mContentValues = new ContentValues();
                            mContentValues.put(KEY_FIELD, msg.split(":")[1]);
                            mContentValues.put(VALUE_FIELD, msg.split(":")[2]);
                            insert(mUri, mContentValues);
                            ackSend.println(ackMsg);
                        } else if(msg.contains("QUERY")){
                            String ackMsg = ACKNOWLEDGE_MESSAGE+"_KEY_VALUE";

                            Cursor resultCursor = query(mUri, null, msg.split(":")[1], null, null);
                            ackMsg = ackMsg + "::" + getKeyValFromCursor((MatrixCursor)resultCursor);
                            Log.i("Server", ackMsg);
                            ackSend.println(ackMsg);
                        } else if(msg.contains("DELETE")){
                            String ackMsg = ACKNOWLEDGE_MESSAGE+"_DELETED";

                            int count = delete(mUri, msg.split(":")[1], null);
                            ackMsg = ackMsg + "::" + String.valueOf(count);
                            Log.i("Server", ackMsg);
                            ackSend.println(ackMsg);
                        } else {
                            ackSend.println(ACKNOWLEDGE_MESSAGE);
                            //publishProgress(msg);
                        }
                    }
                    Log.i("onPostExecute", node.toString());
                }
            } catch (IOException e){
                Log.e(TAG, e.toString());
            } catch (Exception e){
                Log.e(TAG, e.toString());
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {

        }

        @Override
        protected void onPostExecute(Void aVoid) {
            Log.i("onPostExecute", node.toString());
        }
    }


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class ClientTask extends AsyncTask<String, Void, Void>{
        @Override
        protected Void doInBackground(String... params) {

            try {
                Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(params[0]));
                Log.i(TAG, "Client at port : " + params[0]);
                String message = params[1];
                PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
                BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                msgSend.println(message);
                while (!client.isClosed()) {
                    String ack = ackGet.readLine();
                    Log.i(TAG, ack);
                    if(ack.equalsIgnoreCase(ACKNOWLEDGE_MESSAGE)){
                        Log.i(TAG,"Received ack: "+ack);
                        client.close();
                    }else if (ack.contains(ACKNOWLEDGE_MESSAGE)) {
                        String[] contents = ack.split(":");
                        Log.i(TAG, "Received ACK : " + contents[0]);

                        node.predecessor = contents[1];
                        node.successor = contents[2];
                        Log.i("Client task", node.toString());
                        client.close();
                    }
                    Log.i("Stuck","+++++++++++++++");
                }
                //inform predecessor
                client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(node.predecessor) * 2);
                //Log.i(TAG, "Client at port : " + params[0]);
                message = "SUCCESSOR:"+CURRENT_PORT;
                msgSend = new PrintWriter(client.getOutputStream(), true);
                ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                msgSend.println(message);
                while (!client.isClosed()) {
                    String ack = ackGet.readLine();
                    Log.i(TAG, ack);
                    if(ack.equalsIgnoreCase(ACKNOWLEDGE_MESSAGE+"_NODE_MODIFIED")){
                        Log.i("TAG","Received node ack: "+ack);
                        client.close();
                    }
                    Log.i("Stuck","================");
                }

                //inform successor
                client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(node.successor) * 2);
                //Log.i(TAG, "Client at port : " + params[0]);
                message = "PREDECESSOR:"+CURRENT_PORT;
                msgSend = new PrintWriter(client.getOutputStream(), true);
                ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                msgSend.println(message);
                while (!client.isClosed()) {
                    String ack = ackGet.readLine();
                    Log.i(TAG, ack);
                    if(ack.equalsIgnoreCase(ACKNOWLEDGE_MESSAGE+"_NODE_MODIFIED")){
                        Log.i("TAG","Received node ack: "+ack);
                        client.close();
                    }
                    Log.i("Stuck","----------------");
                }

                if (client.isClosed()) {
                    Log.i(TAG, "Received ACK and closed socket");
                } else {
                    Log.i(TAG, "Socket not closed. Verify logic");
                }
            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }

            return null;
        }

    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
