package com.Storm.WritingToAFile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ReadFieldsSpout extends BaseRichSpout{

	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	private boolean completed=false;
	private FileReader fileReader;
	private String str;
	private BufferedReader reader;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		try {
			this.fileReader=new FileReader(conf.get("fileToRead").toString());
		} catch (FileNotFoundException e) {
			 throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
		}
		
		this.collector=collector;
		this.reader=new BufferedReader(fileReader);
	}

	@Override
	public void nextTuple() {
		
		if(!completed) {
			try {
				this.str=reader.readLine();
				if(this.str!=null) {
					this.collector.emit(new Values((Object [])str.split(",")));
				}else {
					completed=true;
					fileReader.close();
				}
			} catch (IOException e) {
				throw new RuntimeException("Error reading tuple",e);
			}
			
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","first_name","last_name","gender","email"));		
	}

}
