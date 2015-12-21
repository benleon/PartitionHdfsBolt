package org.apache.storm.ben;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;


/**
 *
 * @author 
 */
public class TestTopology  
{
    private static final String HDFS_BOLT_ID = "hdfsBolt";
    private static final String LOG_BOLT_ID = "logBolt";
    private static final String DUMMY_SPOUT_ID = "dummyS";
            
    public TestTopology(String configFileLocation) throws Exception 
    {
    	topologyConfig = new Properties();
		try {
			topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
		} catch (FileNotFoundException e) {
			LOG.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		} catch (IOException e) {
			LOG.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		}	    }

   
    
    
    public void configureHDFSBolt(TopologyBuilder builder) 
    {
        // Use pipe as record boundary

        String rootPath = topologyConfig.getProperty("hdfs.path");
        String prefix = topologyConfig.getProperty("hdfs.file.prefix");
        String fsUrl = topologyConfig.getProperty("hdfs.url");
       Float rotationTimeInMinutes = Float.valueOf(topologyConfig.getProperty("hdfs.file.rotation.time.minutes"));

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        //Synchronize data buffer with the filesystem every 100 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // Rotate data files when they reach five MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(20f, FileSizeRotationPolicy.Units.MB);

        //Rotate every X minutes
        //TimedRotationPolicy rotationPolicy = new TimedRotationPolicy
        //            (rotationTimeInMinutes, TimedRotationPolicy.TimeUnit.MINUTES);

        //Hive Partition Action
        //HiveTablePartitionAction hivePartitionAction = new HiveTablePartitionAction
         //           (sourceMetastoreUrl, hiveStagingTableName, databaseName, fsUrl);

        //MoveFileAction moveFileAction = new MoveFileAction().toDestination(rootPath + "/working");



        PartitionFileNameFormat fileNameFormat = new PartitionFileNameFormat()
        				.withPartitionFolder("key=", "")
                        .withPath(rootPath)
                        .withPrefix(prefix);

         //Instantiate the HdfsBolt
        PartitionHdfsBolt hdfsBolt = new PartitionHdfsBolt()
                         .withFsUrl(fsUrl)
                 .withFileNameFormat(fileNameFormat)
                 .withRecordFormat(format)
                 .withRotationPolicy(rotationPolicy)
                 .withSyncPolicy(syncPolicy)
                 .withPartitionField("key");
        //         .addRotationAction(hivePartitionAction);

        //int hdfsBoltCount = Integer.valueOf(topologyConfig.getProperty("hdfsbolt.thread.count"));
        builder.setBolt(HDFS_BOLT_ID, hdfsBolt, 4).shuffleGrouping(DUMMY_SPOUT_ID);
    }
    
   
    public void configureLogBolt(TopologyBuilder builder)
    {
        LogBolt logBolt = new LogBolt();
        builder.setBolt(LOG_BOLT_ID, logBolt).globalGrouping(DUMMY_SPOUT_ID);
    }
    
    public void configureDummySpout(TopologyBuilder builder) 
    {
    	DummySpout dummySpout = new DummySpout();
    	
        //int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
        builder.setSpout(DUMMY_SPOUT_ID, dummySpout);
    }
    

    
    private void buildAndSubmit() throws Exception
    {
        System.out.println("---- Build Topology -----");

        TopologyBuilder builder = new TopologyBuilder();
        configureDummySpout(builder);
        //configureLogTruckEventBolt(builder);
        configureHDFSBolt(builder);
        
        //configureLogBolt(builder);
        
        Config conf = new Config();
        //conf.setDebug(true);
        //conf.setNumWorkers(2);
        //conf.setMaxSpoutPending(5000);
       // LocalCluster cluster = new LocalCluster();
       // cluster.submitTopology("hdfs_partition_test", conf, builder.createTopology());
       // Thread.sleep(1000);
       // cluster.shutdown();
        StormSubmitter.submitTopology("hdfs_partition_test",  conf, builder.createTopology());
    }

    public static void main(String[] str) throws Exception
    {
        System.out.println("---- MY TOPOLOGY -----");

        String configFileLocation = "test.properties";
        System.out.println("DDDHDHDHASHFASFASABEBEBEBEBEBEBEEB");
        TestTopology topology 
                = new TestTopology(configFileLocation);
        topology.buildAndSubmit();
        
      
       
    
    }
    
    private static final Logger LOG = Logger.getLogger(TestTopology.class);

	protected Properties topologyConfig;
	


}
