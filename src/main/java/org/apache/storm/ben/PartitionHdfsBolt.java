package org.apache.storm.ben;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class PartitionHdfsBolt extends MyAbstractHdfsBolt  {
	
    private static final Logger LOG = LoggerFactory.getLogger(PartitionHdfsBolt.class);



	// opened partitioned files
	HashMap<String, PartitionOutputFile> outMap = new HashMap<String, PartitionOutputFile>();
	protected RecordFormat format;
	protected String partitionKey;
	protected PartitionFileNameFormat partitionFileNameFormat = null;
	
	
	public PartitionHdfsBolt withPartitionField(String partitionKey)
	{
		this.partitionKey = partitionKey;
		return this;
	}

	public void writeTuple(Tuple tuple) throws IOException {
		byte[] bytes = this.format.format(tuple);
		String key = tuple.getStringByField(this.partitionKey);
		PartitionOutputFile pf = this.getStream(key);
		FSDataOutputStream out = pf.out;
		out.write(bytes);
		//if (this.outMap)		
	//	out.write(bytes);
		offset += bytes.length;
	}
	
	@Override
	protected void rotateOutputFile() throws IOException {
        LOG.info("Rotating output file...");
        long start = System.currentTimeMillis();
        synchronized (this.writeLock) {
            closeOutputFile();

            LOG.info("Performing {} file rotation actions.", this.rotationActions.size());
            Set<String> keys = this.outMap.keySet();
            for (String key : keys)
        	{	
        		PartitionOutputFile pf = outMap.get(key);
        		for (RotationAction action : this.rotationActions) {
        			
            		action.execute(this.fs, pf.file);       		
            	}
        		//pf.rotateFile(this.partitionFileNameFormat);
        		
            }
            outMap.clear();
            
           
        }
        long time = System.currentTimeMillis() - start;
        LOG.info("File rotation took {} ms.", time);
    }
	
	private PartitionOutputFile getStream(String key) throws IOException {
		//LOG.info("Requesting file for key" + key);
		PartitionOutputFile stream = outMap.get(key);
		if (stream == null) {
			 //LOG.info("Didn't find it Created new file");
			 Path path = new Path(this.partitionFileNameFormat.getPath(key), this.partitionFileNameFormat.getName(this.rotation, System.currentTimeMillis()));
			// LOG.info("Didn't find it Created new file" + path.getName());
			 stream = new PartitionOutputFile(fs, path, key);
			 outMap.put(key, stream);
			 //LOG.info("Didn't find it");

			
		}
	    return stream;
	}
	

	
	
	public PartitionHdfsBolt withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public PartitionHdfsBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public PartitionHdfsBolt withFileNameFormat(PartitionFileNameFormat fileNameFormat){
        this.fileNameFormat = fileNameFormat;
        this.partitionFileNameFormat = fileNameFormat;
        return this;
    }

    public PartitionHdfsBolt withRecordFormat(RecordFormat format){
        this.format = format;
        return this;
    }

    public PartitionHdfsBolt withSyncPolicy(SyncPolicy syncPolicy){
        this.syncPolicy = syncPolicy;
        return this;
    }

    public PartitionHdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy){
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public PartitionHdfsBolt addRotationAction(RotationAction action){
        this.rotationActions.add(action);
        return this;
    }

    public PartitionHdfsBolt withTickTupleIntervalSeconds(int interval) {
        this.tickTupleInterval = interval;
        return this;
    }

    public PartitionHdfsBolt withRetryCount(int fileRetryCount) {
        this.fileRetryCount = fileRetryCount;
        return this;
    }
//
//	@Override
//	public void execute(Tuple arg0) {
//		// TODO Auto-generated method stub
//		super.execute(arg0);
//	}

	@Override
	public
	void closeOutputFile() throws IOException {
		// TODO Auto-generated method stub
		Set<String> keys = outMap.keySet();
		for ( String outKey : keys )
		{
			outMap.get(outKey).out.close();
		}
		//outMap.clear();
		
	}

	@Override
	public
	Path createOutputFile() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	

	@Override
	public void syncTuples() throws IOException {
		Set<String> keys = outMap.keySet();
		for ( String outKey : keys )
		{
			LOG.debug("Attempting to sync all data to filesystem");
			FSDataOutputStream out = outMap.get(outKey).out;
	        if (out instanceof HdfsDataOutputStream) {
	            ((HdfsDataOutputStream) out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
	        } else {
	            out.hsync();
	        }
		}
	}

	@Override
	public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
		 LOG.info("Preparing Partitioned HDFS Bolt...");
	        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);		
	}

}
