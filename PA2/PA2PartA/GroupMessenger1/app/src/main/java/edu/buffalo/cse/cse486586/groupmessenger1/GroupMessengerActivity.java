package edu.buffalo.cse.cse486586.groupmessenger1;

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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;

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
    static final String ACKNOWLEDGE_MESSAGE = "ACK_MSG";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static int SEQUENCE_NUMBER = 0;
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
                    if((msg = reader.readLine()) != null){
                        ackSend.println(ACKNOWLEDGE_MESSAGE);
                        publishProgress(msg);
                    }
                }
            } catch (IOException e){
                Log.e(TAG, e.toString());
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);
            String strReceived = values[0].trim();

            ContentResolver mContentResolver = getContentResolver();
            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
            ContentValues mContentValues = new ContentValues();
            mContentValues.put(KEY_FIELD, Integer.toString(SEQUENCE_NUMBER));
            mContentValues.put(VALUE_FIELD, strReceived);
            mContentResolver.insert(mUri, mContentValues);
            SEQUENCE_NUMBER += 1;

            Log.i(TAG, "Sequence Number : "+SEQUENCE_NUMBER);
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

    private class ClientTask extends AsyncTask<String, Void, Void>{
        @Override
        protected Void doInBackground(String... params) {
            String[] clientPorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
            for(String remote_port : clientPorts) {
                try {
                    Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));
                    Log.i(TAG, "Client at port : " + remote_port);
                    String message = params[0];
                    PrintWriter msgSend = new PrintWriter(client.getOutputStream(), true);
                    BufferedReader ackGet = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    msgSend.println(message);
                    while (!client.isClosed()) {
                        String ack = ackGet.readLine();
                        if (ack.matches(ACKNOWLEDGE_MESSAGE)) {
                            Log.i(TAG, "Received ACK : " + ack);
                            client.close();
                        }
                    }
                    if (client.isClosed()) {
                        Log.i(TAG, "Received ACK and closed socket");
                    } else {
                        Log.i(TAG, "Socket not closed. Verify logic");
                    }
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            return null;
        }
    }
}
