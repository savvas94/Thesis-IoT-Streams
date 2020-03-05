package com.amazonaws.mything;

import com.amazonaws.util.Utilities;
import com.amazonaws.util.Utilities.KeyStorePasswordPair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.iot.client.AWSIotException;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;

public class IoTThing {
	
	private static final Log LOG = LogFactory.getLog(IoTThing.class);
	
	//fetch data from netdata every 10 minutes
	private static int netdataFetchIntervalSecs = 600;
	private static int logIntervalSends = 200;
	private static int sentEntries = 0;
	
	private static AWSIotMqttClient iotThingEntity = null;

	public static void main(String[] args) throws InterruptedException {


		String clientEndpoint = "*****************.iot.us-west-2.amazonaws.com";
		String clientId = "************";
		String certificateFile = "/home/user/AWS/thing/1d060b338d-certificate.pem.crt.sql";
		String privateKeyFile = "/home/user/AWS/thing/1d060b338d-private.pem.key";

		KeyStorePasswordPair pair = Utilities.getKeyStorePasswordPair(certificateFile, privateKeyFile);
		iotThingEntity = new AWSIotMqttClient(clientEndpoint, clientId, pair.keyStore, pair.keyPassword);
		try {
			iotThingEntity.connect();
		} catch (AWSIotException e1) {
			e1.printStackTrace();
		}		
		
		NetdataDataFetcher cpuUsageFetcher = new NetdataDataFetcher();

		//fetch the first available data
		cpuUsageFetcher.fetchCpuUsageDataFromStream();
		long lastFetchTimestamp = System.currentTimeMillis();
		
		//Periodically, fetch new data from Netdata
		//Send one new entry to the stream every second. (netdata by default creates one entry per second)
		int sentBeforeFetch = 0;
		while (true) {
			if ( lastFetchTimestamp < (System.currentTimeMillis() - netdataFetchIntervalSecs*1000) ) {
				cpuUsageFetcher.fetchCpuUsageDataFromStream();
				lastFetchTimestamp = System.currentTimeMillis();
				System.out.println("Sent " + sentBeforeFetch + " entries before fetching new");
				System.out.println("Entries left: "  + cpuUsageFetcher.pendintEntriesCount());
				sentBeforeFetch = 0;
			}
			if(cpuUsageFetcher.pendintEntriesCount() > 0) {
				CpuUsage toSend = cpuUsageFetcher.getNextEntryToProcess();
				sendCpuUsageEntry(toSend);
				sentBeforeFetch++;
			}
			Thread.sleep(700);
		}
	}

	private static void sendCpuUsageEntry(CpuUsage entry) {
		String message = entry.toJsonAsString();
		String topic = "netdata/cpu";		
		try {
			iotThingEntity.publish(topic, AWSIotQos.QOS0, message);
			if (sentEntries%logIntervalSends == 0) {
				LOG.info("Sent entries: " + (sentEntries+1) + " last entry timestamp: " + entry.getTimestamp());
			}
			sentEntries++;
		} catch (AWSIotException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
