package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Properties;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = "GroupMessengerActivity";
    static final String EMPTY_STRING = "";
    static final String NEW_LINE = "\n";
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static ArrayList<String> CLIENT_PORTS = new ArrayList<String>(Arrays.asList(REMOTE_PORT0,
            REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4));
    static final String ACKNOWLEDGE_MESSAGE = "ACK_MSG";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static int S_PtoG = -1;
    static int R_QtoG = -1;
    static String failedClient = "";
    static int failureCount = 0;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TelephonyManager telManager = (TelephonyManager) this.getSystemService(TELEPHONY_SERVICE);
        String portString = telManager.getLine1Number().substring(telManager.getLine1Number().length() - 4);
        final String portNumber = String.valueOf((Integer.parseInt(portString)) * 2);

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (Exception e){
            Log.e(TAG, e.toString());
        }
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final Button sendButton = (Button) findViewById(R.id.button4);
        sendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText editText = (EditText) findViewById(R.id.editText1);
                Log.i(TAG, "From EditText1 : " +editText.getText().toString());
                String msg = editText.getText().toString();
                //tv.append(msg + NEW_LINE);
                editText.setText(EMPTY_STRING);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, portNumber);
                Log.i(TAG, "Client "+portNumber+" created");
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
    private class ClientMessages implements Comparable<ClientMessages>{
        String clientPort;
        String message;
        int seqNum;
        boolean deliverable;
        public ClientMessages(String clientPort, String message, int seqNum, boolean deliverable){
            this.clientPort = clientPort;
            this.message = message;
            this.seqNum = seqNum;
            this.deliverable = deliverable;
        }

        @Override
        public int compareTo(ClientMessages anotherClient) {
            if (seqNum < anotherClient.seqNum)
                return -1;
            else if (seqNum > anotherClient.seqNum)
                return 1;
            else {
                if(!deliverable && anotherClient.deliverable)
                    return -1;
                else if(!anotherClient.deliverable && deliverable)
                    return 1;
                else {
                    if (Integer.parseInt(clientPort) < Integer.parseInt(anotherClient.clientPort))
                        return -1;
                    else
                        return 1;
                }
            }
        }
    }

    public void printQueue(PriorityQueue<ClientMessages> clientMessages){
        ClientMessages head = null;
        head = clientMessages.peek();
        Iterator<ClientMessages> itr = clientMessages.iterator();
        Log.i("Queue", "Printing queue");
        while(itr.hasNext()) {
            head = itr.next();
            Log.i("Queue", "Head: " + head.message + " Client: " + head.clientPort
                    + " Seq: " + head.seqNum + " deliverable: " + head.deliverable);
        }
        Log.i("Queue", "End of queue");
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            ArrayList<Integer> rcvSeqNums = new ArrayList<Integer>();
            int sugSeqNum = -1;
            int failAdjFlag = 1;
            int sendFlag = 0;
            PriorityQueue<ClientMessages> clientMessages = new PriorityQueue<ClientMessages>();
            try{
                while(true){
                    Socket client = serverSocket.accept();
                    //Log.i("test", client.toString());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter ackSend = new PrintWriter(client.getOutputStream(), true);
                    String msg;
                    if((msg = reader.readLine()) != null){
                        Log.i("reader", msg);
                        String clientPort = msg.split(":")[0];
                        //Log.i("testing", clientPort);
                        if(CLIENT_PORTS.contains(clientPort)) {
                            sugSeqNum = 1 + (R_QtoG > sugSeqNum ? R_QtoG : sugSeqNum);
                            clientMessages.add(new ClientMessages(clientPort,
                                    msg.split(":")[1], Integer.parseInt(msg.split(":")[2]), false));
                            ackSend.println(ACKNOWLEDGE_MESSAGE + ":" + (sugSeqNum));
                            printQueue(clientMessages);
                        }
                        else {
                            Log.i("Received", "In else R_QtoG = " + R_QtoG + " Message = " + msg);
                            int seq = Integer.parseInt(msg.split(":")[1]);
                            String rcvMsg = msg.split(":")[0];
                            clientPort = msg.split(":")[2];
                            Iterator<ClientMessages> msgItr = clientMessages.iterator();
                            ClientMessages clientMsg = null;
                            while (msgItr.hasNext()){
                                clientMsg = msgItr.next();
                                if(clientMsg.message.equals(rcvMsg))
                                    break;
                            }
                            printQueue(clientMessages);
                            Log.i("Queue", "Removing message: "+clientMsg.message);
                            clientMessages.remove(clientMsg);
                            if(!(failedClient.length() > 0 && clientPort.equals(failedClient)))
                                clientMessages.add(new ClientMessages(clientPort, rcvMsg, seq, true));
                            printQueue(clientMessages);
                            ackSend.println(ACKNOWLEDGE_MESSAGE);
                        }

                        deliverMessages(clientMessages);

                    }

                }

            } catch (IOException e){
                Log.e(TAG, e.toString());
            }
            return null;
        }

        private void deliverMessages(PriorityQueue<ClientMessages> clientMessages){
            ClientMessages head = null;
            while ((head = clientMessages.peek()) != null && head.deliverable) {
                //Log.i("checking", "Message: "+ head.message + " Port: "+head.clientPort+" Seq: " +head.seqNum);
                R_QtoG += 1;

                Log.i("Received", " Publishing "+Integer.parseInt(head.clientPort)/2+"'s Message: " + head.message
                        + " seq: " + head.seqNum + " when R_StoQ: " + R_QtoG);
                publishProgress(head.message+":"+R_QtoG);
                clientMessages.remove();
            }
            if((head = clientMessages.peek()) != null && !head.deliverable
                    && failedClient.length() > 0 && head.clientPort.equals(failedClient)){
                Log.i("Remove","Message: "+head.message);
                clientMessages.remove(head);
            }
        }

        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);
            String strReceived = values[0].split(":")[0].trim();
            String seq = values[0].split(":")[1].trim();
            ContentResolver mContentResolver = getContentResolver();
            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
            ContentValues mContentValues = new ContentValues();
            //R_QtoG += 1;
            mContentValues.put(KEY_FIELD, seq);
            mContentValues.put(VALUE_FIELD, strReceived);
            mContentResolver.insert(mUri, mContentValues);

            Log.i("delivered", "key : "+seq+" value : "+strReceived);
            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived + NEW_LINE);

        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... params) {
            int connClients = 0;
            ArrayList<String> clients = new ArrayList<String>();

            S_PtoG += 1;
            int suggestedSeqNum = getSuggestedSeqNum(params[0],params[1]);
            Log.i("sequence","Message: "+params[0]+" New Seq Num: "+String.valueOf(suggestedSeqNum));
            int retries = 1;

            for (String remote_port : CLIENT_PORTS) {
                try {
                    Socket client = new Socket();
                    client.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port)));
                    client.setSoTimeout(500);
                    Log.i("Sending", "Sending to client: " + remote_port);
                    String message = params[0];
                    PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
                    BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    connClients += 1;
                    clients.add(remote_port);
                    msgSend.println(message + ":" + suggestedSeqNum + ":" + params[1]);
                    while (!client.isClosed()) {
                        String ack = ackGet.readLine();
                        if (ack.contains(ACKNOWLEDGE_MESSAGE)) {
                            if (ack.length() == ACKNOWLEDGE_MESSAGE.length()) {
                                //Log.i("testing", "Received ACK : " + ack);
                                client.close();
                            }
                        } else if (null != ackGet.readLine()) {
                            //Log.i(TAG, "Data not read. Closing socket and moving to the next one.");
                            client.close();
                        }
                    }
                    if (client.isClosed()) {
                        Log.i(TAG, "Got ACK and closed socket");
                    } else {
                        Log.i(TAG, "Socket not closed. Verify logic");
                    }
                    Thread.sleep(500);
                } catch (NullPointerException e) {
                    failedClient = remote_port;
                    failureCount += 1;
                    //CLIENT_PORTS.remove(remote_port);
                    Log.i("Failure", "Client " + failedClient);
                } catch (Exception e) {
                    Log.e("Sending message", e.toString());
                }
            }

            if(failedClient.length() > 0 && CLIENT_PORTS.contains(failedClient))
                CLIENT_PORTS.remove(failedClient);
            return null;
        }


        int getSuggestedSeqNum(String message, String portNumber) {
            ArrayList<Integer> newSeqNumList = new ArrayList<Integer>();
            int connClients = 0;
            for (String remote_port : CLIENT_PORTS) {
                try {
                    Socket client = new Socket();
                    client.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port)));
                    client.setSoTimeout(500);
                    //Log.i(TAG, "Client at port : " + remote_port);
                    //String message = params[0];
                    PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
                    BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    connClients += 1;
                    //clients.add(remote_port);
                    msgSend.println(portNumber + ":" + message + ":" + S_PtoG);
                    while (!client.isClosed()) {
                        String ack = ackGet.readLine();
                        if (ack.contains(ACKNOWLEDGE_MESSAGE)) {
                            if (ack.length() > ACKNOWLEDGE_MESSAGE.length()) {
                                int newSeqNum = Integer.parseInt(ack.split(":")[1]);
                                newSeqNumList.add(newSeqNum);
                                //Log.i("testing", "Added: " + String.valueOf(newSeqNum) + ". " + String.valueOf(newSeqNumList.size()));

                            }
                        } else if (null != ackGet.readLine()) {
                            Log.i(TAG, "Data not read. Closing socket and moving to the next one.");
                            client.close();
                        }
                    }
                    if (client.isClosed()) {
                        Log.i(TAG, "Received ACK and closed socket");
                    } else {
                        Log.i(TAG, "Socket not closed. Verify logic");
                    }
                    Thread.sleep(250);
                } catch (Exception e) {
                    Log.e("Seq Num", e.toString());
                    //e.printStackTrace();
                }
            }
            String seqNums = "";
            for(int x : newSeqNumList)
                seqNums = seqNums + ", " + String.valueOf(x);
            Log.i("sequence","Suggested seq for "+message+ " = ["+seqNums+"]");
            return Collections.max(newSeqNumList);
        }
    }
}
