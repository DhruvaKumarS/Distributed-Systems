package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnLDumpClickListener implements OnClickListener {

	private static final String TAG = OnLDumpClickListener.class.getName();
	private static final int TEST_CNT = 50;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private final TextView mTextView;
	private final ContentResolver mContentResolver;
	private final Uri mUri;
	private final ContentValues[] mContentValues;

    static String displayText = "";
    static int queryCount = 0;

	public OnLDumpClickListener(TextView _tv, ContentResolver _cr) {
		mTextView = _tv;
		mContentResolver = _cr;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		mContentValues = initTestValues();
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private ContentValues[] initTestValues() {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
			cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
			cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
		}

		return cv;
	}

	@Override
	public void onClick(View v) {
		new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
	}

	private class Task extends AsyncTask<Void, String, Void> {

		@Override
		protected Void doInBackground(Void... params) {
			if (testQuery()) {
				publishProgress("Query success\n"+queryCount);
                queryCount = 0;
                displayText = "";
			} else {
				publishProgress("Query fail\n");
			}
			
			return null;
		}
		
		protected void onProgressUpdate(String...strings) {
			mTextView.append(strings[0]);

			return;
		}

		private boolean testQuery() {
			try {
				Cursor resultCursor = mContentResolver.query(mUri, null,
							"@", null, null);
				Log.i("LDump", String.valueOf(resultCursor.getCount()));
                resultCursor.moveToFirst();
                if(resultCursor.getCount() >0) {
                    do {
                        displayText += resultCursor.getString(0) + ":" + resultCursor.getString(1) + "\n";
                        //Log.i("LDump", resultCursor.getString(0));
                        queryCount += 1;
                    } while (resultCursor.moveToNext());
                }
                resultCursor.close();

			} catch (Exception e) {
                Log.e("LDump", e.toString());
				return false;
			}

			return true;
		}
	}
}
