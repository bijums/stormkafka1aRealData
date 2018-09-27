package com.ibs.storm;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.joda.time.DateTime;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

/**
 * @author 
 */
public class LoggerBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(LoggerBolt.class);
	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

	public void execute(Tuple input, BasicOutputCollector collector) {
		//LOG.info(String.valueOf(input.getString(0)));
		//System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&    "+String.valueOf(input.getString(0)));
		System.out.println("KEY    "+String.valueOf(input.getValueByField("KEY")));
		System.out.println("OFFSET    "+String.valueOf(input.getValueByField("OFFSET")));
		System.out.println("VALUE    "+input.getValueByField("VALUE"));
		/*Map<String, Object> obj = ImmutableMap.<String, Object>of(
				"timestamp", formatter.format(new Date()),
				"propType", "SPAVL",
				"propcode", "PRCSJCCB",
				"dtOarvl", "2019-01-11",
				"lenOstay", "1",
				"numOadult","1",
				"clientId","CCN560520175",
				"click", "30"
				);*/
		
		/*Map<String, Object> obj = ImmutableMap.<String, Object>of(
				"timestamp", formatter.format(new Date()),
				"propType", "SPAVL",
				"propcode", "PRCSJCCB",
				"dtOarvl", "2019-01-11",
				"numOrooms", "1"
				
				);
		collector.emit(new Values(obj));*/
		
								/*		Map<String, Object> obj = ImmutableMap.<String, Object>of(
										"timestamp", formatter.format(new Date()),
										"propType", "barVal",
										"advertiser", "abc",
										"click", "30"
										);*/
								/*		Map<String, Object> obj = ImmutableMap.<String, Object>of(
										"timestamp", formatter.format(new Date()),
										"propType", "SPAVL",
										"click", "30"
										);*/
								/*Map<String, Object> obj = ImmutableMap.<String, Object>of(
										"timestamp", formatter.format(new Date()),
										"propType", "SPAVL",
										"propcode", "PRCSJCCB",
										"dtOarvl", "2019-01-11",
										"lenOstay", "1",
										"numOadult","1",
										"numOrooms","1",
										"clientId","CCN560520175",
										"click", "30"
										);
										collector.emit(new Values(obj));*/
		ObjectMapper mapper  = new ObjectMapper();
		Map<String, Object> obj = null;
		try {
			obj = mapper.readValue((String)input.getValueByField("VALUE"), HashMap.class);

		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (obj!=null){
			System.out.println("Emitting log bolt "+obj);
			collector.emit(new Values(obj));
		}	
		collector.emit(new Values(input.getValueByField("VALUE")));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("event"));
	}
/*	public static void main(String[] args) {
		ObjectMapper mapper  = new ObjectMapper();
		Map<String, Object> obj = null;
		try {
			obj = mapper.readValue("{\"timestamp\":\"2018-02-01T18:01:35Z\",\"propType\":\"SPAVL\",\"click\":6}", HashMap.class);
			System.out.println(obj);

		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
*/
}