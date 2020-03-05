package com.amazonaws.mything;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.mything.CpuUsage;

/**
 * Reads data from a Netdata server and serves them one by one.
 */

public class NetdataDataFetcher {
	
	private List<CpuUsage> notProcessededEntries;
	private long mostRecentEntry;
	
	public NetdataDataFetcher() {
		notProcessededEntries = new LinkedList<>();
	}
	
	/**
	 * Sends a GET Request to the Rest API of NetData, gets all the entries from there, and puts them in a list sorted by timestamp.
	 * If there are duplicates between older and newer entries in the list, the duplicates are removed.
	 */
	public void fetchCpuUsageDataFromStream() {
		
		try {
			URL url = new URL("http://snf-848555.vm.okeanos.grnet.gr:19999/api/v1/data?chart=system.cpu");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}

			/* GET request is inserted into a BufferedReader. This creates a StringBuilder, and all this
			 * becomes a JSONObject. Then, we only keep the "data" value, which has a JSONArray with the 
			 * actual CPU Usage data for each timestamp. 
			 */
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			StringBuilder sb = new StringBuilder();
			String output;
			JSONObject json;
			while ((output = br.readLine()) != null) {
				sb.append(output);
			}
			json = new JSONObject(sb.toString());
			JSONArray json2 = json.getJSONArray("data");

			/* Every field of the JSONArray has the data for a particular timestamp.
			 * We create a CpuUsage object for each one, and put them in a LinkedList, sorted by timestamp.
			 */
			List<CpuUsage> newEntries = new LinkedList<>();
			int newEntriesCounter = 0;
			for (int i = 0; i < json2.length(); i++) {
				
				JSONArray line = json2.getJSONArray(i);
				CpuUsage entry = new CpuUsage((int) line.get(0), (double) line.getDouble(1), (double) line.getDouble(2),
						(double) line.getDouble(3), (double) line.getDouble(4), (double) line.getDouble(5),
						(double) line.getDouble(6), (double) line.getDouble(7), (double) line.getDouble(8),
						(double) line.getDouble(9));
				
				//only keep entries that are more recent than the entries we already have.
				if(entry.getTimestamp() > mostRecentEntry) {
					newEntries.add(0, entry);
					newEntriesCounter++;
				}
			}
			/* Put all the new entries in the list together with the older entries.
			 * The new entries are put in the end of the list, so the final list is always sorted.
			 */
			notProcessededEntries.addAll(newEntries);
			
			//update most recent entry
			mostRecentEntry = notProcessededEntries.get(notProcessededEntries.size()-1).getTimestamp();
			
			conn.disconnect();
			System.out.println();
			System.out.println("Fetched " + newEntriesCounter + " new entries.");
			System.out.println();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	/**
     * Returns the oldest entry and removes it from the list.
     *
     */
    public CpuUsage getNextEntryToProcess() {    	
        return notProcessededEntries.remove(0);
    }
    
    public List<CpuUsage> getAvailableEntries() {
    	return notProcessededEntries;
    }
    
    public int pendintEntriesCount() {
    	return notProcessededEntries.size();
    }

}
