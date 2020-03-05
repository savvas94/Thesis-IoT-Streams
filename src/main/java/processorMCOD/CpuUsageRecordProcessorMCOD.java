package processorMCOD;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instance;

import moa.clusterers.outliers.MCOD.MCOD;

import com.amazonaws.netdataresttest.model.CpuUsage;

/**
 * Processes records retrieved from cpuUsage stream.
 *
 */
public class CpuUsageRecordProcessorMCOD implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(CpuUsageRecordProcessorMCOD.class);
    private String kinesisShardId;

    // Reporting stats interval
    private static final long REPORTING_INTERVAL_MILLIS = 120000L; // 2 minutes
    private long nextReportingTimeInMillis;

    // Checkpointing interval
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L; // 1 minute
    private long nextCheckpointTimeInMillis;

    // Runs MCOD algorithm for the data given by the AWS stream    
    private MCOD mcod;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        
        this.initializeMCOD();
    }
    
    private void initializeMCOD() {
    	mcod = new MCOD();
        mcod.kOption.setValue(8);
        mcod.radiusOption.setValue(5);
        mcod.windowSizeOption.setValue(120);        
        mcod.prepareForUse();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {    	
        for (Record record : records) {
            processRecord(record);
        }

        // If it is time to report stats as per the reporting interval, report stats
        if (System.currentTimeMillis() > nextReportingTimeInMillis) {
            reportOutliersStats();
            nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        }

        // Checkpoint once every checkpoint interval
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    private void reportOutliersStats() {
    	System.out.println(mcod.getStatistics());
    	mcod.PrintOutliers();
    }

    private void resetStats() {
    	mcod.resetLearning();
    }

    private void processRecord(Record record) {
        CpuUsage entry = CpuUsage.fromJsonAsBytes(record.getData().array());
        if (entry == null) {
            LOG.warn("Skipping record. Unable to parse record into CpuUsage. Partition Key: " + record.getPartitionKey());
            return;
        }
        mcod.processNewInstanceImpl(getInstanceFromCpuUsage(entry));
    }
    
	private Instance getInstanceFromCpuUsage(CpuUsage entry) {		
		double[] values = new double[9];
		values[0] = entry.getGuest_nice();
		values[1] = entry.getGuest();
		values[2] = entry.getSteal();
		values[3] = entry.getSoftirq();
		values[4] = entry.getIrq();
		values[5] = entry.getUser();
		values[6] = entry.getSystem();
		values[7] = entry.getNice();
		values[8] = entry.getIowait();		
        Instance inst = new DenseInstance(1.0, values);        
        return inst;
	}

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
    	LOG.info("Checkpointing shard " + kinesisShardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
        	LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
	@Override
	public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
		LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
	}


}
