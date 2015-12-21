package org.apache.storm.ben;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionOutputFile {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionOutputFile.class);
	
	Path file;
	FSDataOutputStream out;
	String key = "";
	// opened partitioned files
	
//	public long offset = 0;
   // public int rotation = 0;
//    PartitionHdfsBolt father;
   // public Integer tickTupleInterval = 0;
   // protected ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();
    protected  FileSystem fs;
    public PartitionOutputFile(FileSystem fs, Path path, String key) throws IOException
    {
    	this.fs = fs;
    	 //Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
    	this.file = path;
		out = this.fs.create(path);
		this.key = key;

    }
    
//    public void rotateFile(PartitionFileNameFormat fileNameFormat, String key) throws IOException
//    {
//    	rotation ++;
//    	 file = new Path(fileNameFormat.getPath(key), fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
//    	 out = this.fs.create(file);
//    }
}
