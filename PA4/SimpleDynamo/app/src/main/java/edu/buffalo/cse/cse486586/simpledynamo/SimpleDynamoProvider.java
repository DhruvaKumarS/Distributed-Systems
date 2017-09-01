package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.sql.Timestamp;

import edu.buffalo.cse.cse486586.simpledynamo.Ring;
import edu.buffalo.cse.cse486586.simpledynamo.Node;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

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
	Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

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
	static Set<String> keyInsertedList = new TreeSet<String>();
	static PriorityQueue<String> nodeIDs = new PriorityQueue<String>();
	static HashMap<String, String> nodePortMap = new HashMap<String, String>();
	static final String GET_NODE = "GET_NODE";
	static final String SEND_NODE = "SEND_NODE";
    static boolean INSERT_REPLICA = false;
    static boolean QUERY_REPLICA = false;
    static boolean DELETE_REPLICA = false;
    static Ring dynamoRing;
    static boolean QUERY_WAIT = false;
    private static Object INSERT_LOCK = new Object();
    private static Object QUERY_LOCK = new Object();
    private static Object DELETE_LOCK = new Object();
    static final String INSTRUCTION = "INSTRUCTION";
    static final String INSERT = "INSERT";
    static final String REPLICA = "REPLICA";
    static final String REJOIN = "REJOIN";
    static final String SOURCE = "SOURCE";
    static HashMap<String,String> predecessorMap = new HashMap<String, String>();
    static HashMap<String,String> predecessor2Map = new HashMap<String, String>();
    static HashMap<String,String> currentMap = new HashMap<String, String>();
    static HashMap<String,String> successorMap = new HashMap<String, String>();
    static HashMap<String,String> successor2Map = new HashMap<String, String>();
    static HashMap<String,String> insertedMap = new HashMap<String, String>();
    static ArrayList<String> rejoinList = new ArrayList<String>();
	static Node node;
    static String[] existingFiles;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		//FileInputStream inputStream;
		//String[] columnNames = {KEY_FIELD, VALUE_FIELD};
		//String fileName;
        int count = 0;
        if(selection.compareTo("EXISTING") == 0 && existingFiles != null && existingFiles.length > 0){
            Log.i("delete", "Deleting existing files");
            for(String fileName : existingFiles){
                getContext().deleteFile(fileName);
            }
            return count;
        }
        else {
            String fileContent;
            if (!selection.contains("PORT_")) {
                selection = "PORT_" + CURRENT_PORT + "&" + selection;
            }
            //byte[] content = new byte[50];
            //MatrixCursor matrixCursor = new MatrixCursor(columnNames);
            ArrayList<String> filesToDelete = new ArrayList<String>();
            try {
                //fileName = KEY_FIELD + Integer.toString(i);
                Log.i("delete", selection);
                Log.i("delete", selection.split("&")[0]);
                //synchronized (DELETE_LOCK){
                if (selectionArgs != null && selectionArgs[0].compareTo(REPLICA) == 0) {
                    if (selection.split("&")[1].equals(LOCAL_DHT)) {
                        keyInsertedList.addAll(Arrays.asList(existingFiles));
                        filesToDelete.addAll(keyInsertedList);
                    } else
                        filesToDelete.add(selection.split("&")[1]);
                    for (String fileName : filesToDelete) {
                        //Log.i(TAG, "Reading file: "+fileName);
                        getContext().deleteFile(fileName);
                        keyInsertedList.remove(fileName);
                        Node destinationNode = dynamoRing.getDestinationNode(genHash(fileName));
                        if (destinationNode.nodeID.compareTo(node.nodeID) == 0) {
                            currentMap.remove(fileName);
                        } else if (destinationNode.successor.compareTo(node.nodeID) == 0) {
                            predecessorMap.remove(fileName);
                        } else if (destinationNode.successor2.compareTo(node.nodeID) == 0) {
                            predecessor2Map.remove(fileName);
                        }
                        count += 1;
                    }
                    //DELETE_REPLICA = false;
                    Log.i("delete", "Deleted replica: " + count);
                } else {
                    if (selection.split("&")[1].equals(GLOBAL_DHT)) {
                        keyInsertedList.addAll(Arrays.asList(existingFiles));
                        filesToDelete.addAll(keyInsertedList);
                        if (node.getHashedNodeID().compareTo(node.getHashedSuccessor()) == 0
                                || selection.contains(node.successor)) {
                            for (String fileName : filesToDelete) {
                                //Log.i(TAG, "Reading file: "+fileName);
                                getContext().deleteFile(fileName);
                                keyInsertedList.remove(fileName);
                                currentMap.remove(fileName);
                                count += 1;
                            }

                        } else {
                            ArrayList<String> otherNodesList = dynamoRing.getOtherNodesList(node);
                            count = deleteKeys(otherNodesList, LOCAL_DHT, "DELETE_NODE:");

                            for (String fileName : filesToDelete) {
                                //Log.i(TAG, "Reading file: "+fileName);
                                getContext().deleteFile(fileName);
                                currentMap.remove(fileName);
                                count += 1;
                            }

                            Log.i("delete", "Delete: " + count);
                        }
                    } else if (selection.split("&")[1].equals(LOCAL_DHT)) {
                        keyInsertedList.addAll(Arrays.asList(existingFiles));
                        filesToDelete.addAll(keyInsertedList);
                        for (String fileName : filesToDelete) {
                            //Log.i(TAG, "Reading file: "+fileName);
                            getContext().deleteFile(fileName);
                            keyInsertedList.remove(fileName);
                            currentMap.remove(fileName);
                            count += 1;
                        }
                    } else {
                        Log.i("delete", selection.split("&")[1]);
                        String hashFileName = genHash(selection.split("&")[1]);
                        Node destinationNode = dynamoRing.getDestinationNode(hashFileName);
                        if (destinationNode.nodeID.compareTo(node.nodeID) == 0) {
                            ArrayList<String> destinationNodeStr = new ArrayList<String>();
                            filesToDelete.add(selection.split("&")[1]);
                            for (String fileName : filesToDelete) {
                                //Log.i(TAG, "Reading file: "+fileName);
                                getContext().deleteFile(fileName);
                                currentMap.remove(fileName);
                                count += 1;
                            }
                            destinationNodeStr.add(node.successor);
                            successorMap.remove(selection.split("&")[1]);
                            destinationNodeStr.add(node.successor2);
                            successor2Map.remove(selection.split("&")[1]);
                            count += deleteKeys(destinationNodeStr, selection.split("&")[1], "DELETE_REPLICA:");
                            Log.i("delete", "Deleted: " + count);
                        } else {
                            ArrayList<String> destinationNodeStr = new ArrayList<String>();
                            destinationNodeStr.add(destinationNode.nodeID);
                            boolean deleteFromThreeNodes = false;
                            if (destinationNode.successor.compareTo(node.nodeID) == 0) {
                                destinationNodeStr.add(destinationNode.successor2);
                                predecessorMap.remove(selection.split("&")[1]);
                            } else if (destinationNode.successor2.compareTo(node.nodeID) == 0) {
                                destinationNodeStr.add(destinationNode.successor);
                                predecessor2Map.remove(selection.split("&")[1]);
                            } else {
                                deleteFromThreeNodes = true;
                                destinationNodeStr.add(destinationNode.successor);
                                destinationNodeStr.add(destinationNode.successor2);
                            }

                            Log.i(TAG, "Deleting " + selection.split("&")[1] + " from " + destinationNode.nodeID);
                            count = deleteKeys(destinationNodeStr, selection, "DELETE_NODE:");
                            if (!deleteFromThreeNodes) {
                                filesToDelete.add(selection.split("&")[1]);
                                for (String fileName : filesToDelete) {
                                    //Log.i(TAG, "Reading file: "+fileName);
                                    getContext().deleteFile(fileName);
                                    keyInsertedList.remove(fileName);
                                    currentMap.remove(fileName);
                                    count += 1;
                                }
                            }
                    /*for(String fileName : keyInsertedList){
                        if(fileName.compareTo(selection.split("&")[1]) == 0) {
                            Log.i(TAG, "Deleting replica: " + fileName);
                            getContext().deleteFile(fileName);
                        }
                    }*/
                        }


                    }
                }
                //}
            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }
            //Log.i(TAG, Integer.toString(matrixCursor.getCount()));
            Log.v("delete", String.valueOf(count));
        }
		return count;
	}

	private int deleteKeys(ArrayList<String> nodeList,String selection, String instruction) {
        int count = 0;
        //String instruction = "DELETE_NODE:";
        try {
            for(String nodeStr : nodeList) {
                Log.i("deleteKeys", "Deleting "+selection+" in "+nodeStr);
                Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(nodeStr) * 2);
                //Log.i(TAG, "Client at port : " + params[0]);
                String message = instruction + selection;
                PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
                BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                msgSend.println(message);
                String contents = "";
                while (!client.isClosed()) {
                    String ack = ackGet.readLine();
                    if(ack == null){
                        client.close();
                        continue;
                    }
                    Log.i(TAG, ack);
                    if (ack.contains(ACKNOWLEDGE_MESSAGE + nodeStr + "_DELETED")) {
                        if (ack.length() > (ACKNOWLEDGE_MESSAGE + nodeStr + "_DELETED::").length()) {
                            contents = ack.split("::")[1];
                        } else {
                            contents = "";
                        }

                        client.close();
                    }
                }
                Log.i("deleteKeys", contents);
                if (contents != null && contents.length() > 0)
                    count += Integer.parseInt(contents);
                instruction = "DELETE_REPLICA:";
            }
        }catch (Exception e){
            Log.e("deleteKeys", e.toString());
        }
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
        String instruction = (String) values.get(INSTRUCTION);
        String sourcePort = (String) values.get(SOURCE);
		//Log.i(TAG, "Key : "+fileName);
		//Log.i(TAG, "Value : "+fileContent);

		try {
			String hashFileName = genHash(fileName);
            Node destinationNode = dynamoRing.getDestinationNode(hashFileName);
            Log.i("insert","Inserting "+fileName+" in "+destinationNode.nodeID);
            //synchronized (INSERT_LOCK) {
                if(instruction != null && (instruction.compareTo(REPLICA) == 0 || instruction.compareTo(REJOIN) == 0)){
                    //INSERT_REPLICA = false;
                    Log.v("insert", "Inserting "+fileName+" from other node");
                    FileOutputStream outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                    outputStream.write(fileContent.getBytes());
                    outputStream.close();
                    keyInsertedList.add(fileName);
                    insertedMap.put(fileName, fileContent+"@"+System.currentTimeMillis());
                    if(destinationNode.successor.compareTo(node.nodeID) == 0){
                        predecessorMap.put(fileName, fileContent+"@"+System.currentTimeMillis());
                    } else if(destinationNode.successor2.compareTo(node.nodeID) == 0){
                        predecessor2Map.put(fileName, fileContent+"@"+System.currentTimeMillis());
                    } else if(destinationNode.nodeID.compareTo(node.nodeID) == 0){
                        currentMap.put(fileName, fileContent+"@"+System.currentTimeMillis());
                        //String message = "INSERT_REPLICA_" + CURRENT_PORT + ":" + fileName + ":" + fileContent;
                        ArrayList<String> destinationNodeStr = new ArrayList<String>();
                        /*if(node.successor2.compareTo(sourcePort) == 0) {
                            destinationNodeStr.add(node.successor);
                        }
                        else if(node.successor.compareTo(sourcePort) == 0) {
                            destinationNodeStr.add(node.successor2);
                        }
                        else{
                            destinationNodeStr.add(node.successor);
                            destinationNodeStr.add(node.successor2);
                        }*/
                        successorMap.put(fileName, fileContent+"@"+System.currentTimeMillis());
                        successor2Map.put(fileName, fileContent+"@"+System.currentTimeMillis());
                        if(instruction.compareTo(REPLICA) == 0) {
                            destinationNodeStr.add(node.successor);
                            destinationNodeStr.add(node.successor2);
                            sendKey(destinationNodeStr, fileName, fileContent);
                        }
                    }
                }
                else {
                    while (QUERY_WAIT);
                    if (destinationNode.nodeID.compareTo(node.nodeID) == 0) {
                        Log.v("insert", "Inserting " + fileName + " from current node");
                        FileOutputStream outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                        outputStream.write(fileContent.getBytes());
                        outputStream.close();
                        keyInsertedList.add(fileName);
                        insertedMap.put(fileName, fileContent+"@"+System.currentTimeMillis());
                        currentMap.put(fileName, fileContent+"@"+System.currentTimeMillis());
                        //String message = "INSERT_REPLICA_" + CURRENT_PORT + ":" + fileName + ":" + fileContent;
                        //String sendToNodes = node.successor + ":" + node.successor2;
                        ArrayList<String> destinationNodeStr = new ArrayList<String>();
                        destinationNodeStr.add(node.successor);
                        successorMap.put(fileName, fileContent+"@"+System.currentTimeMillis());
                        destinationNodeStr.add(node.successor2);
                        successor2Map.put(fileName, fileContent+"@"+System.currentTimeMillis());
                        sendKey(destinationNodeStr,fileName,fileContent);
                        /*new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                node.successor , message);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                node.successor2 , message);*/
                        Log.i("insert", "Inserted in if: " + fileName);
                    } else {
                        Log.i(TAG, "Sending " + fileName + " to " + destinationNode.nodeID);
                        //sendKey(fileName, fileContent, node);
                        //String message = "INSERT_REPLICA_" + CURRENT_PORT + ":" + fileName + ":" + fileContent;
                        //String sendToNodes = destinationNode.nodeID + ":";
                        ArrayList<String> destinationNodeStr = new ArrayList<String>();
                        destinationNodeStr.add(destinationNode.nodeID);
                        boolean sentToThreeNodes = false;
                        if(destinationNode.successor.compareTo(node.nodeID) == 0) {
                            //sendToNodes += destinationNode.successor2;
                            predecessorMap.put(fileName, fileContent+"@"+System.currentTimeMillis());
                            destinationNodeStr.add(destinationNode.successor2);
                            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            //        destinationNode.successor2, message);
                        }
                        else if(destinationNode.successor2.compareTo(node.nodeID) == 0) {
                            //sendToNodes += destinationNode.successor;
                            predecessor2Map.put(fileName, fileContent+"@"+System.currentTimeMillis());
                            destinationNodeStr.add(destinationNode.successor);
                            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            //        destinationNode.successor, message);
                        }
                        else {
                            //sendToNodes += destinationNode.successor + ":" + destinationNode.successor2;
                            destinationNodeStr.add(destinationNode.successor);
                            destinationNodeStr.add(destinationNode.successor2);
                            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            //        destinationNode.successor, message);
                            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            //        destinationNode.successor2, message);
                            sentToThreeNodes = true;
                        }
                        /*new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                sendToNodes, message);*/
                        if(!sentToThreeNodes){
                            FileOutputStream outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                            outputStream.write(fileContent.getBytes());
                            outputStream.close();
                            keyInsertedList.add(fileName);
                            insertedMap.put(fileName, fileContent+"@"+System.currentTimeMillis());
                            //currentMap.put(fileName, fileContent);
                        }
                        Log.i("insert","Sending to "+Arrays.toString(destinationNodeStr.toArray()));
                        sendKey(destinationNodeStr,fileName,fileContent);

                    }
                }
            //}
			//Log.i(TAG, String.valueOf(keyInsertedList.size()));
		} catch (Exception e) {
			Log.e("insert", "File write failed : " +fileName);
			e.printStackTrace();
		}
		Log.v("insert", values.toString());
		return uri;
	}

	private void sendKey(ArrayList<String> nodeList, String fileName, String fileContent) {
        String message = "INSERT_REPLICA_" + CURRENT_PORT + ":" + fileName + ":" + fileContent;
        for(String nodeStr : nodeList) {
            try {
                Log.i("sendKey", "Sending " + fileName + " to " + nodeStr);
                Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(nodeStr) * 2);
                //Log.i(TAG, "Client at port : " + params[0]);
                client.setSoTimeout(500);
                PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
                BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                msgSend.println(message);
                String contents = "";
                while (!client.isClosed()) {
                    String ack = ackGet.readLine();
                    if(ack == null){
                        client.close();
                        continue;
                    }
                    Log.i("sendKey", ack);
                    if (ack.contains(ACKNOWLEDGE_MESSAGE + nodeStr + "_REPLICATED")) {
                        client.close();
                        //return;
                    }
                }

            } catch (Exception e) {
                Log.e("sendKey", e.toString());
                e.printStackTrace();
            }
        }
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {

        //QUERY_WAIT = true;
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
			Log.i("query", selection);
            //synchronized (QUERY_LOCK){
                if(selectionArgs != null && selectionArgs[0].compareTo("QUERY_REPLICA") == 0){
                    //QUERY_REPLICA = false;
                    if (selection.split("&")[1].equals(LOCAL_DHT)) {
                        keyInsertedList.addAll(Arrays.asList(existingFiles));
                        filesToRead.addAll(keyInsertedList);
                    }
                    else
                        filesToRead.add(selection.split("&")[1]);

                    matrixCursor = getKeys(filesToRead);
                    Log.i("query", "Found replica: "+matrixCursor.getCount());
                    Log.i("query", "Contents replica: "+getKeyValFromCursor(matrixCursor));
                }
                else {
                    if (selection.split("&")[1].equals(GLOBAL_DHT)) {
                        keyInsertedList.addAll(Arrays.asList(existingFiles));
                        filesToRead.addAll(keyInsertedList);
                        if (node.getHashedNodeID().compareTo(node.getHashedSuccessor()) == 0
                                || selection.contains(node.successor)) {
                            matrixCursor = getKeys(filesToRead);

                        } else {
                            ArrayList<String> otherNodesList = dynamoRing.getOtherNodesList(node);
                            matrixCursor = fetchKeys(otherNodesList, LOCAL_DHT);
                            //if (selection.contains(node.nodeID)) {
                            Log.i("query", "From other nodes: " + String.valueOf(matrixCursor.getCount()));
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
                            Log.i("query", "From " + CURRENT_PORT + " and other nodes : " + String.valueOf(matrixCursor.getCount()));
                            Log.i("query", "Found: " + getKeyValFromCursor(matrixCursor));
                        }
                    } else if (selection.split("&")[1].equals(LOCAL_DHT)) {
                        while(QUERY_WAIT);
                        keyInsertedList.addAll(Arrays.asList(existingFiles));
                        filesToRead.addAll(keyInsertedList);
                        matrixCursor = getKeys(filesToRead);
                    } else {
                        Log.i("query", selection.split("&")[1]);
                        String hashFileName = genHash(selection.split("&")[1]);
                        Node destinationNode = dynamoRing.getDestinationNode(hashFileName);
                        if (destinationNode.nodeID.compareTo(node.nodeID) == 0) {
                            //filesToRead.add(selection.split("&")[1]);
                            //matrixCursor = getKeys(filesToRead);
                            ArrayList<String> destinationNodeStr = new ArrayList<String>();
                            destinationNodeStr.add(destinationNode.nodeID);
                            destinationNodeStr.add(destinationNode.successor);
                            destinationNodeStr.add(destinationNode.successor2);
                            Log.i("query", "Fetching " + selection.split("&")[1] + " from "
                                    + destinationNode.nodeID + ", "
                                    + destinationNode.successor + ", "
                                    + destinationNode.successor2);
                            matrixCursor = fetchKeys(destinationNodeStr, selection);
                            Log.i("query", "Found: " + matrixCursor.getCount());
                            Log.i("query", "Contents: " + getKeyValFromCursor(matrixCursor));
                        } /*else if(QUERY_REPLICA){
                    filesToRead.add(selection.split("&")[1]);
                    matrixCursor = getKeys(filesToRead);
                    QUERY_REPLICA = false;
                    Log.i("query", "Found replica: "+matrixCursor.getCount());
                    Log.i("query", "Contents replica: "+getKeyValFromCursor(matrixCursor));
                }*/     else {
                            ArrayList<String> destinationNodeStr = new ArrayList<String>();

                            boolean queryFromThreeNodes = false;
                            /*if(destinationNode.successor.compareTo(node.nodeID) == 0)
                                destinationNodeStr.add(destinationNode.successor2);
                            else if(destinationNode.successor2.compareTo(node.nodeID) == 0) {
                                destinationNodeStr.add(destinationNode.successor);
                                filesToRead.add(selection.split("&")[1]);
                                matrixCursor = getKeys(filesToRead);
                            }
                            else {
                                filesToRead.add(selection.split("&")[1]);
                                destinationNodeStr.add(destinationNode.successor2);
                                destinationNodeStr.add(destinationNode.successor);
                                destinationNodeStr.add(destinationNode.nodeID);
                                queryFromThreeNodes = true;
                            }*/
                            destinationNodeStr.add(destinationNode.successor2);
                            destinationNodeStr.add(destinationNode.successor);
                            destinationNodeStr.add(destinationNode.nodeID);
                            Log.i("query", "Fetching " + selection.split("&")[1] + " from "
                                    + destinationNode.nodeID + ", "
                                    + destinationNode.successor + ", "
                                    + destinationNode.successor2);
                            matrixCursor = fetchKeys(destinationNodeStr, selection);

                            /*if(!queryFromThreeNodes){
                                filesToRead.add(selection.split("&")[1]);
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
                            }
                            else {

                            }*/
                        }

                    }
                }
            //}
		}
		catch (Exception e){
			Log.e("query", e.toString());
		}
		//Log.i(TAG, Integer.toString(matrixCursor.getCount()));
		Log.v("query", selection);
        //QUERY_WAIT = false;
		return matrixCursor;
	}

	private MatrixCursor fetchKeys(ArrayList<String> nodeList, String selection) {
		//String[] columnNames = {KEY_FIELD, VALUE_FIELD};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);
		String[] keyValPairs = null;
        String instruction = "QUERY_REPLICA:";
        StringBuilder contents = new StringBuilder();
        for (String nodeStr : nodeList) {
            if(nodeStr.compareTo(node.nodeID) == 0 && !QUERY_WAIT){
                ArrayList<String> filesToRead = new ArrayList<String>();
                filesToRead.add(selection.split("&")[1]);
                String result = getKeyValFromCursor(getKeys(filesToRead));
                if(result != null && result.contains(":"))
                    contents.append(result + ":");
            }
            else try {

                Log.i("query", "Querying " + selection + " in " + nodeStr);
                Socket client = new Socket();
                client.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(nodeStr) * 2));
                //Log.i(TAG, "Client at port : " + params[0]);
                String message = instruction + selection;
                PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
                BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                msgSend.println(message);

                while (!client.isClosed()) {
                    String ack = ackGet.readLine();
                    if(ack == null) {
                        client.close();
                        //instruction = "QUERY_REPLICA:";
                        continue;
                    }

                    Log.i("fetchKeys", ack);
                    if (ack.contains(ACKNOWLEDGE_MESSAGE + nodeStr + "_KEY_VALUE")) {
                        if (ack.length() > (ACKNOWLEDGE_MESSAGE + nodeStr + "_KEY_VALUE::").length()) {
                            contents.append(ack.split("::")[1]+":");
                        } else {
                            contents.append("");
                        }

                        client.close();
                    }
                }

                /*instruction = "QUERY_REPLICA:";
                if(matrixCursor.getCount() > 0 && selection.compareTo(LOCAL_DHT) != 0){
                    break;
                }*/
            } catch (Exception e) {
                Log.e("fetchKeys", e.toString());
                e.printStackTrace();
            }
        }
        int ctr = 0;
        if(contents.length() > 0){
            contents.deleteCharAt(contents.length() - 1);
        }
        keyValPairs = contents.length() > 0 ? contents.toString().split(":") : null;
        Log.i("fetchKeys", contents.toString());
        //String queryKey = "";
        //String queryValue = "";
        HashMap<String, String> keyValMap = new HashMap<String, String>();
        while (keyValPairs != null && keyValPairs.length > 0 && ctr < keyValPairs.length) {
            if(selection.compareTo(LOCAL_DHT) != 0) {
                if(keyValMap.containsKey(keyValPairs[ctr + 1])){
                    MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
                    mRowBuilder.add(columnNames[0], keyValMap.get(keyValPairs[ctr + 1]));
                    mRowBuilder.add(columnNames[1], keyValPairs[ctr + 1]);
                    break;
                }
                else if(ctr + 2 >= keyValPairs.length){
                    MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
                    mRowBuilder.add(columnNames[0], keyValPairs[ctr]);
                    mRowBuilder.add(columnNames[1], keyValPairs[ctr + 1]);
                    break;
                }
                else {
                    keyValMap.put(keyValPairs[ctr + 1], keyValPairs[ctr]);
                }
                //break;
            }
            else{
                MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
                mRowBuilder.add(columnNames[0], keyValPairs[ctr]);
                mRowBuilder.add(columnNames[1], keyValPairs[ctr + 1]);

            }
            ctr += 2;
        }
		return matrixCursor;
	}

	private String getKeyValFromCursor(MatrixCursor matrixCursor) {
		//Log.i("getKeyValFromCursor","Here");
		StringBuilder currentKeyValPairs = new StringBuilder();
		try {
			int keyIndex = matrixCursor.getColumnIndex(KEY_FIELD);
			int valueIndex = matrixCursor.getColumnIndex(VALUE_FIELD);
			matrixCursor.moveToFirst();
			if (matrixCursor.getCount() > 0) {
				do {
					String returnKey = matrixCursor.getString(keyIndex);
					String returnValue = matrixCursor.getString(valueIndex);
					currentKeyValPairs.append(returnKey + ":" + returnValue + ":");
					//Log.i("getKeyValFromCursor", currentKeyValPairs);
				} while (matrixCursor.moveToNext());

				matrixCursor.close();
				if (currentKeyValPairs.length() > 0) {
					currentKeyValPairs.deleteCharAt(currentKeyValPairs.length() - 1);
				}
			}
		} catch(Exception e){
			currentKeyValPairs = new StringBuilder();
            currentKeyValPairs.append("");
			return currentKeyValPairs.toString();
		}
		//matrixCursor.moveToFirst();
        if(currentKeyValPairs.length() == 0)
            currentKeyValPairs.append("");
		return currentKeyValPairs.toString();
	}

	private MatrixCursor getKeys(ArrayList<String> filesToRead){
		FileInputStream inputStream;
		//String[] columnNames = {KEY_FIELD, VALUE_FIELD};
		//String fileName;
		String fileContent;
		byte[] content = new byte[50];
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        //while(QUERY_WAIT);
		for(String fileName : filesToRead){
			try {
				Log.i("getKeys", "Reading file: " + fileName);
				inputStream = getContext().openFileInput(fileName);
				//inputStream.read();
				int length = inputStream.read(content);
				//Log.i(TAG, "content length : "+length);
				fileContent = new String(content).substring(0, length);
				Log.i("getKeys", "fileContent : " + fileContent);
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

    private String getKeyValFromMap(HashMap<String,String> keyValMap){
        StringBuilder keyValStr = new StringBuilder();
        for(Map.Entry<String,String> keyVal : keyValMap.entrySet()){
            keyValStr.append(keyVal.getKey()+":"+keyVal.getValue()+":");
        }
        if(keyValStr.length() > 0)
            keyValStr.deleteCharAt(keyValStr.length() - 1);
        return keyValStr.toString();
    }

	@Override
	public boolean onCreate() {

		TelephonyManager telManager = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portString = telManager.getLine1Number().substring(telManager.getLine1Number().length() - 4);
		final String portNumber = String.valueOf((Integer.parseInt(portString)) * 2);
		CURRENT_PORT = portString;
		//node = new Node();
		//node.nodeID = portString;
		//node.successor = portString;
		//node.predecessor = portString;
		try{
			//Log.i(TAG, Integer.parseInt(genHash(portNumber), 16));
			MAX_NODE_ID = genHash(PORT0);
			nodePortMap.put(genHash(PORT0), PORT0);
			nodePortMap.put(genHash(PORT1), PORT1);
			nodePortMap.put(genHash(PORT2), PORT2);
			nodePortMap.put(genHash(PORT3), PORT3);
			nodePortMap.put(genHash(PORT4), PORT4);

            nodeIDs.add(genHash(PORT4));
            nodeIDs.add(genHash(PORT1));
            nodeIDs.add(genHash(PORT0));
            nodeIDs.add(genHash(PORT2));
            nodeIDs.add(genHash(PORT3));

            dynamoRing = new Ring(nodeIDs, nodePortMap);
            node = dynamoRing.getCurrentNode(CURRENT_PORT);
            rejoinList.add(node.successor);
            rejoinList.add(node.successor2);
            rejoinList.add(node.predecessor);
            rejoinList.add(node.predecessor2);
            Log.i(TAG,"Done");
            //System.exit(0);
            /*for(int i = 0; i < 5; i++)
                getBI(genHash(remotePortList[i]));*/
            /*Iterator itr = nodePortMap.entrySet().iterator();
            String nodes = "";
            while(itr.hasNext()){
                nodes += itr.next() + "\n";
            }*/
			Log.i(TAG, "Nodes-Port hashmap: "+ Arrays.asList(nodePortMap));
            existingFiles = getContext().fileList();
            //Log.i("ExistingFiles", existingFiles != null && existingFiles.length > 0 ? existingFiles[0] : "Empty");
            /*if(existingFiles != null && existingFiles.length > 0){
                //delete(mUri, "EXISTING", null);
                FileInputStream inputStream;
                String fileContent;
                byte[] content = new byte[50];
                for(String fileName : existingFiles) {
                    try {
                        //Log.i("getKeys", "Reading file: " + fileName);
                        inputStream = getContext().openFileInput(fileName);
                        //inputStream.read();
                        int length = inputStream.read(content);
                        //Log.i(TAG, "content length : "+length);
                        fileContent = new String(content).substring(0, length);
                        insertedMap.put(fileName, fileContent);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }*/
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            QUERY_WAIT = true;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, null, "REJOIN_"+CURRENT_PORT);
			/*if(!portNumber.equals(REMOTE_PORT0)){
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REMOTE_PORT0, CURRENT_PORT);
			} else{
				nodeIDs.add(genHash(PORT0));
			}*/
            //nodeRejoinInsert();
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
						if(msg.contains("INSERT_NODE")) {
							String ackMsg = ACKNOWLEDGE_MESSAGE+CURRENT_PORT+"_INSERTED";

							ContentValues mContentValues = new ContentValues();
							mContentValues.put(KEY_FIELD, msg.split(":")[1]);
							mContentValues.put(VALUE_FIELD, msg.split(":")[2]);
                            mContentValues.put(INSTRUCTION,INSERT);
                            mContentValues.put(SOURCE,msg.split(":")[0].split("_")[2]);
							insert(mUri, mContentValues);
							ackSend.println(ackMsg);
						} else if(msg.contains("INSERT_REPLICA")){
                            String ackMsg = ACKNOWLEDGE_MESSAGE+CURRENT_PORT+"_REPLICATED";
                            INSERT_REPLICA = true;
                            ContentValues mContentValues = new ContentValues();
                            mContentValues.put(KEY_FIELD, msg.split(":")[1]);
                            mContentValues.put(VALUE_FIELD, msg.split(":")[2]);
                            mContentValues.put(INSTRUCTION,REPLICA);
                            mContentValues.put(SOURCE,msg.split(":")[0].split("_")[2]);
                            insert(mUri, mContentValues);
                            INSERT_REPLICA = false;
                            ackSend.println(ackMsg);
                        } else if(msg.contains("QUERY_NODE")){
							StringBuilder ackMsg = new StringBuilder(ACKNOWLEDGE_MESSAGE+CURRENT_PORT+"_KEY_VALUE");

							Cursor resultCursor = query(mUri, null, msg.split(":")[1], null, null);
							ackMsg.append("::" + getKeyValFromCursor((MatrixCursor)resultCursor));
							Log.i("Server", ackMsg.toString());
							ackSend.println(ackMsg.toString());
						} else if(msg.contains("QUERY_REPLICA")){
                            StringBuilder ackMsg = new StringBuilder(ACKNOWLEDGE_MESSAGE+CURRENT_PORT+"_KEY_VALUE");
                            //QUERY_REPLICA = true;
                            String[] queryReplica = {"QUERY_REPLICA"};
                            Cursor resultCursor = query(mUri, null, msg.split(":")[1], queryReplica, null);
                            ackMsg.append("::" + getKeyValFromCursor((MatrixCursor)resultCursor));
                            Log.i("Server", ackMsg.toString());
                            ackSend.println(ackMsg.toString());
                        } else if(msg.contains("DELETE_NODE")){
							StringBuilder ackMsg = new StringBuilder(ACKNOWLEDGE_MESSAGE+CURRENT_PORT+"_DELETED");

							int count = delete(mUri, msg.split(":")[1], null);
							ackMsg.append("::" + String.valueOf(count));
							Log.i("Server", ackMsg.toString());
							ackSend.println(ackMsg.toString());
						} else if(msg.contains("DELETE_REPLICA")){
                            StringBuilder ackMsg = new StringBuilder(ACKNOWLEDGE_MESSAGE+CURRENT_PORT+"_DELETED");
                            //DELETE_REPLICA = true;
                            String[] replica = {REPLICA};
                            int count = delete(mUri, msg.split(":")[1], replica);
                            ackMsg.append("::" + String.valueOf(count));
                            Log.i("Server", ackMsg.toString());
                            ackSend.println(ackMsg.toString());
                        } else if(msg.contains("REJOIN")){
                            StringBuilder ackMsg = new StringBuilder(ACKNOWLEDGE_MESSAGE+CURRENT_PORT+"_MISSED_KEY_VALUE");
                            String rejoinedNode = msg.split("_")[1];
                            StringBuilder keyValPairs = new StringBuilder();
                            Log.i("Server", "Rejoined node: "+rejoinedNode);
                            if(node.predecessor.compareTo(rejoinedNode) == 0){
                                Log.i("Server","Successor");
                                keyValPairs.append(getKeyValFromMap(predecessorMap));
                                predecessorMap.clear();
                            } else if(node.predecessor2.compareTo(rejoinedNode) == 0){
                                Log.i("Server","Successor2");
                                keyValPairs.append(getKeyValFromMap(predecessor2Map));
                                predecessor2Map.clear();
                            } else if(node.successor.compareTo(rejoinedNode) == 0){
                                Log.i("Server","Predecessor");
                                keyValPairs.append(getKeyValFromMap(currentMap));
                                if(keyValPairs.length() != 0 && keyValPairs.toString().contains(":"))
                                    keyValPairs.append(":");
                                keyValPairs.append(getKeyValFromMap(successorMap));
                                currentMap.clear();
                                successorMap.clear();
                            } else{
                                Log.i("Server","Predecessor2");
                                keyValPairs.append(getKeyValFromMap(currentMap));
                                if(keyValPairs.length() != 0 && keyValPairs.toString().contains(":"))
                                    keyValPairs.append(":");
                                keyValPairs.append(getKeyValFromMap(successor2Map));
                                currentMap.clear();
                                successor2Map.clear();
                            }
                            if(keyValPairs == null || keyValPairs.length() == 0)
                                keyValPairs.append("");
                            ackMsg.append("::" + keyValPairs.toString());
                            Log.i("Server", ackMsg.toString());
                            ackSend.println(ackMsg.toString());
                            //Thread.sleep(1000);
                        } else {
							ackSend.println(ACKNOWLEDGE_MESSAGE);
							//publishProgress(msg);
						}
					}
					Log.i("onPostExecute", node.toString());
				}
			} catch (IOException e){
				Log.e("Server", e.toString());
                e.printStackTrace();
			} catch (Exception e){
				Log.e("Server", e.toString());
                e.printStackTrace();
			}
			return null;
		}

		@Override
		protected void onProgressUpdate(String... values) {

		}

		@Override
		protected void onPostExecute(Void aVoid) {
			//Log.i("onPostExecute", node.toString());
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
            for(String remotePort : rejoinList) {
                try {
                /*ArrayList<String> portsToConnect = new ArrayList<String>();
                if(params[0].split(":").length > 1){
                    portsToConnect.addAll(Arrays.asList(params[0].split(":")));
                }
                else{
                    portsToConnect.add(params[0]);
                }*/

                    Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort) * 2);
                    Log.i("Client", "Client at port : " + remotePort);
                    String message = params[1];
                    PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
                    BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    msgSend.println(message);
                    while (!client.isClosed()) {
                        String ack = ackGet.readLine();
                        if(ack == null){
                            client.close();
                            continue;
                        }
                        Log.i("Client", ack);
                        if (ack.contains(ACKNOWLEDGE_MESSAGE + remotePort + "_MISSED_KEY_VALUE")) {
                            Log.i("nodeRejoinInsert", "Received ack: " + ack);
                            client.close();
                            if (ack.split("::").length > 1) {
                                String[] keyVals = ack.split("::")[1].split(":");
                                int ctr = 0;
                                int len = keyVals.length;
                                while (ctr + 1 < len) {
                                    if(!insertedMap.containsKey(keyVals[ctr])
                                            || (insertedMap.containsKey(keyVals[ctr])
                                                && Long.parseLong(insertedMap.get(keyVals[ctr]).split("@")[1])
                                                    < Long.parseLong(keyVals[ctr + 1].split("@")[1]))) {
                                        ContentValues mContentValues = new ContentValues();
                                        mContentValues.put(KEY_FIELD, keyVals[ctr]);
                                        mContentValues.put(VALUE_FIELD, keyVals[ctr + 1].split("@")[0]);
                                        mContentValues.put(INSTRUCTION, REJOIN);
                                        insert(mUri, mContentValues);
                                    }
                                    ctr += 2;
                                }
                            }
                        } else if (ack.contains(ACKNOWLEDGE_MESSAGE + remotePort)) {
                            Log.i("Client", "Received ack: " + ack);
                            client.close();
                        } /*else if (ack.contains(ACKNOWLEDGE_MESSAGE)) {
                            String[] contents = ack.split(":");
                            Log.i(TAG, "Received ACK : " + contents[0]);

                            node.predecessor = contents[1];
                            node.successor = contents[2];
                            Log.i("Client task", node.toString());
                            client.close();
                        }*/
                        //Log.i("Stuck", "+++++++++++++++");
                    }


                    if (client.isClosed()) {
                        Log.i(TAG, "Received ACK and closed socket");
                    } else {
                        Log.i(TAG, "Socket not closed. Verify logic");
                    }

                    //Thread.sleep(1000);
                } catch (Exception e) {
                    Log.e("Client", e.toString());
                }
            }
            QUERY_WAIT = false;
			return null;
		}

	}


	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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
