package com.ibs.storm.common;

import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeamConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.typeclass.Timestamper;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.storm.druid.bolt.DruidBeamFactory;
import org.apache.storm.task.IMetricsContext;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;

/**
 * Druid bolt must be supplied with a BeamFactory. You can implement one of these using the
 * [DruidBeams builder's] (https://github.com/druid-io/tranquility/blob/master/core/src/main/scala/com/metamx/tranquility/druid/DruidBeams.scala)
 * "buildBeam()" method. See the [Configuration documentation] (https://github.com/druid-io/tranquility/blob/master/docs/configuration.md) for details.
 * For more details refer [Tranquility library] (https://github.com/druid-io/tranquility) docs.
 */
public class SampleDruidBeamFactoryImpl implements DruidBeamFactory<Map<String, Object>> {
    Map<String, Object> factoryConf = null;


    public SampleDruidBeamFactoryImpl(Map<String, Object> factoryConf) {
        this.factoryConf = factoryConf; // This can be used to pass config values
    }

    public Beam<Map<String, Object>> makeBeam(Map<?, ?> conf, IMetricsContext metrics) {


        final String indexService = "druid/overlord"; // Your overlord's druid.service
        final String discoveryPath = "/druid/discovery"; // Your overlord's druid.discovery.curator.path
        final String dataSource = "testFlowDS1";
        final List<String> dimensions = ImmutableList.of("propType","propcode","ts", "lenOstay","numOrooms","numOadult","clientId");
        //final List<String> dimensions = ImmutableList.of("timestamp","propType","propcode", "dtOarvl", "lenOstay","numOadult","numOrooms","clientId");
        //final List<String> dimensions = ImmutableList.of("propType");
        //final List<String> dimensions = ImmutableList.of("publisher", "advertiser");
        List<AggregatorFactory> aggregator = ImmutableList.<AggregatorFactory>of(
                new CountAggregatorFactory(
                        "numOrooms"
                )
        );
        // Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
        final Timestamper<Map<String, Object>> timestamper = new Timestamper<Map<String, Object>>()
        {
            public DateTime timestamp(Map<String, Object> theMap)
            {
                return new DateTime(theMap.get("ts"));
            }
        };

        // Tranquility uses ZooKeeper (through Curator) for coordination.
        final CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString((String)conf.get("druid.tranquility.zk.connect")) // we can use Storm conf to get config values
                .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
                .build();
        curator.start();

        // The JSON serialization of your object must have a timestamp field in a format that Druid understands. By default,
        // Druid expects the field to be called "timestamp" and to be an ISO8601 timestamp.
        final TimestampSpec timestampSpec = new TimestampSpec("ts", "auto", null);

        // Tranquility needs to be able to serialize your object type to JSON for transmission to Druid. By default this is
        // done with Jackson. If you want to provide an alternate serializer, you can provide your own via ```.objectWriter(...)```.
        // In this case, we won't provide one, so we're just using Jackson.
        final Beam<Map<String, Object>> beam = DruidBeams
                .builder(timestamper)
                .curator(curator)
                .discoveryPath(discoveryPath)
                .location(DruidLocation.create(indexService, dataSource))
                .timestampSpec(timestampSpec)
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregator, QueryGranularities.MINUTE))
                .tuning(
                        ClusteredBeamTuning
                                .builder()
                                .segmentGranularity(Granularity.HOUR)
                                .windowPeriod(new Period("PT10M"))
                                .partitions(1)
                                .replicants(1)
                                .build()
                )
                .druidBeamConfig(
                        DruidBeamConfig
                                .builder()
                                .indexRetryPeriod(new Period("PT10M"))
                                .build())
                .buildBeam();

        return beam;
    }
}