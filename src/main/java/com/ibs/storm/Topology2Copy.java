package com.ibs.storm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.druid.bolt.DruidBeamBolt;
import org.apache.storm.druid.bolt.DruidBeamFactory;
import org.apache.storm.druid.bolt.DruidConfig;
import org.apache.storm.druid.bolt.ITupleDruidEventMapper;
import org.apache.storm.druid.bolt.TupleDruidEventMapper;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.ibs.storm.common.SampleDruidBeamFactoryImpl;



/* * @author 
 * This is the main topology class. 
 * All the spouts and bolts are linked together and is submitted on to the cluster*/

public class Topology2Copy {
	
	private void submitTopology() throws Exception {
		KafkaSpoutRetryService kafkaSpoutRetryService =  new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
				KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
		//KafkaSpoutConfig spoutConf =  KafkaSpoutConfig.builder("sandbox-hdp.hortonworks.com:6667", "pipelineTopic")
				KafkaSpoutConfig spoutConf =  KafkaSpoutConfig.builder("sandbox-hdp.hortonworks.com:6667", "realDataTopic")
				.setGroupId("consumerGroupId")
				.setOffsetCommitPeriodMs(10_000)
				.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
				.setMaxUncommittedOffsets(10_000)
				.setRetry(kafkaSpoutRetryService)
				.setRecordTranslator
				(consumerRecord ->  {
		            return Arrays.asList(consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
		        }, new Fields("OFFSET", "KEY", "VALUE"))
				.build();
		TopologyBuilder builder = new TopologyBuilder();
		
		

		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.put("druid.tranquility.zk.connect","localhost:2181");
		conf.setDebug(true);
		
        DruidBeamFactory druidBeamFactory = new SampleDruidBeamFactoryImpl(new HashMap<String, Object>());
        DruidConfig.Builder dConfigbuilder = DruidConfig.newBuilder().discardStreamId(DruidConfig.DEFAULT_DISCARD_STREAM_ID);
        ITupleDruidEventMapper<Map<String, Object>> eventMapper = new TupleDruidEventMapper(TupleDruidEventMapper.DEFAULT_FIELD_NAME);
        DruidBeamBolt<Map<String, Object>> druidBolt = new DruidBeamBolt<Map<String, Object>>(druidBeamFactory, eventMapper, dConfigbuilder);
        	
        System.out.println("KAFKA PROPS:--->"+spoutConf.getKafkaProps());
        builder.setSpout("kafka-spout",  new KafkaSpout(spoutConf), 1);
        builder.setBolt("log-messages", new LoggerBolt()).shuffleGrouping("kafka-spout");	
        builder.setBolt("druid-bolt", druidBolt).shuffleGrouping("log-messages");		
		//StormSubmitter.submitTopology("testFlow", conf, builder.createTopology());
		StormSubmitter.submitTopology("testFlow2", conf, builder.createTopology());
	}

	public static void main(String[] args) throws Exception {
		String configFile;		
		
		Topology2Copy ingestionTopology = new Topology2Copy();
		ingestionTopology.submitTopology();
	}
}
