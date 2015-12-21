package org.apache.storm.ben;

import java.util.Map;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;

import backtype.storm.task.TopologyContext;


/**
 * Partitioned File Format allowing to write to partitioned folders 
 *
 * @author bleonhardi
 *
 */
public class PartitionFileNameFormat implements FileNameFormat{


	// parameters for partitioning scheme
	//private String partitionKey = "";
	private String partitionPrefix = "";
	private String partitionPostfix = "";
	
	 private String componentId;
	 private int taskId;
	 private String path = "/storm";
	 private String prefix = "";
	 private String extension = ".txt";
	
	/**
	 * This method partitions the data by a folder that contains a fixed pre and
	 * postfix and a Input arguments myfolder/timestamp/
	 * 
	 * @return the bolt with a partition folder
	 */
	public PartitionFileNameFormat withPartitionFolder(String prefix, String postfix){
		//this.partitionKey = key;
		this.partitionPrefix = prefix;
		this.partitionPostfix = postfix;
        return this;
    }

	public String getPartitionFolderName(String tupleValue) {
		return partitionPrefix + tupleValue + partitionPostfix;
	}
	
	public String getPath(String partitionKey){
        return this.getPath() + "/" + this.getPartitionFolderName(partitionKey);
    }
	
	/**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public PartitionFileNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    /**
     * Overrides the default file extension.
     *
     * @param extension
     * @return
     */
    public PartitionFileNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public PartitionFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
    }

    @Override
    public String getName(long rotation, long timeStamp) {
        return this.prefix + this.componentId + "-" + this.taskId +  "-" + rotation + "-" + timeStamp + this.extension;
    }

    public String getPath(){
        return this.path;
    }
	
	
	
}
