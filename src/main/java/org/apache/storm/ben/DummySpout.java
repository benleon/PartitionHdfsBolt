package org.apache.storm.ben;


	import backtype.storm.spout.SpoutOutputCollector;
	import backtype.storm.task.TopologyContext;
	import backtype.storm.topology.OutputFieldsDeclarer;
	import backtype.storm.topology.base.BaseRichSpout;
	import backtype.storm.tuple.Fields;
	import backtype.storm.tuple.Values;
	import backtype.storm.utils.Utils;

	import java.util.Map;
	import java.util.Random;

	public class DummySpout extends BaseRichSpout  {
	  SpoutOutputCollector _collector;
	  int currentValue = 0;
	  String[] keyArray = new String[]{"2010", "2011"};
	   String[] valueArray = new String[]{"This is the first line", "This is the second line", "This is the third line"};
	 
	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    _collector = collector;
	    //_rand = new Random();
	  }

	  @Override
	  public void nextTuple() {
		  if (currentValue > 500000000) {
			  
			 _collector.reportError(new Exception("Enough is enough"));
		  }
	    //Utils.sleep(100);
	     String key = keyArray[currentValue % 2];
	    String value = valueArray[currentValue % 3] + currentValue;
	    currentValue++;
	    _collector.emit(new Values(key, value));
	  }

	  @Override
	  public void ack(Object id) {
	  }

	  @Override
	  public void fail(Object id) {
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("key", "value"));
	  }

	}