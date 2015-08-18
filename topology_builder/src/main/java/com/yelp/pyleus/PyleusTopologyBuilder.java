package com.yelp.pyleus;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.error.YAMLException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.KeyValueSchemeAsMultiScheme;
import storm.kafka.SpoutConfig;
import storm.kafka.StringKeyValueScheme;
import storm.kafka.ZkHosts;

import com.yelp.pyleus.bolt.PythonBolt;
import com.yelp.pyleus.spec.BoltSpec;
import com.yelp.pyleus.spec.ComponentSpec;
import com.yelp.pyleus.spec.SpoutSpec;
import com.yelp.pyleus.spec.TopologySpec;
import com.yelp.pyleus.spout.PythonSpout;

public class PyleusTopologyBuilder {

    public static final String YAML_FILENAME = "/resources/pyleus_topology.yaml";
    public static final String KAFKA_ZK_ROOT_FMT = "/pyleus-kafka-offsets/%s";
    public static final String KAFKA_BROKER_ZK_PATH = "/brokers/";
    public static final String KAFKA_CONSUMER_ID_FMT = "pyleus-%s";
    public static final String MSGPACK_SERIALIZER_CLASS = "com.yelp.pyleus.serializer.MessagePackSerializer";

    public static final PythonComponentsFactory pyFactory = new PythonComponentsFactory();

    public static void handleBolt(final TopologyBuilder builder, final BoltSpec spec,
        final TopologySpec topologySpec) {

        PythonBolt bolt = pyFactory.createPythonBolt(spec.module,
                spec.options, topologySpec.logging_config, topologySpec.serializer);

        if (spec.output_fields != null) {
            bolt.setOutputFields(spec.output_fields);
        }

        if (spec.tick_freq_secs != -1.f) {
            bolt.setTickFreqSecs(spec.tick_freq_secs);
        }

        IRichBolt stormBolt = bolt;

        BoltDeclarer declarer;
        if (spec.parallelism_hint != -1) {
            declarer = builder.setBolt(spec.name, stormBolt, spec.parallelism_hint);
        } else {
            declarer = builder.setBolt(spec.name, stormBolt);
        }

        if (spec.tasks != -1) {
            declarer.setNumTasks(spec.tasks);
        }

        for (Map<String, Object> grouping : spec.groupings) {
            Map.Entry<String, Object> entry = grouping.entrySet().iterator().next();
            String groupingType = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> groupingMap = (Map<String, Object>) entry.getValue();
            String component = (String) groupingMap.get("component");
            String stream = (String) groupingMap.get("stream");

            if (groupingType.equals("shuffle_grouping")) {
                declarer.shuffleGrouping(component, stream);
            } else if (groupingType.equals("global_grouping")) {
                declarer.globalGrouping(component, stream);
            } else if (groupingType.equals("fields_grouping")) {
                @SuppressWarnings("unchecked")
                List<String> fields = (List<String>) groupingMap.get("fields");
                String[] fieldsArray = fields.toArray(new String[fields.size()]);
                declarer.fieldsGrouping(component, stream, new Fields(fieldsArray));
            } else if (groupingType.equals("local_or_shuffle_grouping")) {
                declarer.localOrShuffleGrouping(component, stream);
            } else if (groupingType.equals("none_grouping")) {
                declarer.noneGrouping(component, stream);
            } else if (groupingType.equals("all_grouping")) {
                declarer.allGrouping(component, stream);
            } else {
                throw new RuntimeException(String.format("Unknown grouping type: %s", groupingType));
            }
        }
    }

    public static void handleSpout(final TopologyBuilder builder, final SpoutSpec spec,
        final TopologySpec topologySpec) {

        IRichSpout spout;
        if (spec.type.equals("kafka")) {
            spout = handleKafkaSpout(builder, spec);
        } else {
            spout = handlePythonSpout(builder, spec, topologySpec);
        }

        SpoutDeclarer declarer;
        if (spec.parallelism_hint != -1) {
            declarer = builder.setSpout(spec.name, spout, spec.parallelism_hint);
        } else {
            declarer = builder.setSpout(spec.name, spout);
        }

        if (spec.tasks != -1) {
            declarer.setNumTasks(spec.tasks);
        }
    }

    public static IRichSpout handleKafkaSpout(
            @SuppressWarnings("unused") final TopologyBuilder builder,
            final SpoutSpec spec) {
        String topic = (String) spec.options.get("topic");
        if (topic == null) {
            throw new RuntimeException("Kafka spout must have topic");
        }

        String zkHosts = (String) spec.options.get("zk_hosts");
        if (zkHosts == null) {
            throw new RuntimeException("Kafka spout must have zk_hosts");
        }

        String zkRoot = (String) spec.options.get("zk_root");
        if (zkRoot == null) {
            zkRoot = String.format(KAFKA_ZK_ROOT_FMT, spec.name);
        }
        
        String brokerZkPath = (String) spec.options.get("broker_zk_path");
        if (brokerZkPath == null) {
        	brokerZkPath = KAFKA_BROKER_ZK_PATH;
        }
        
        String consumerId = (String) spec.options.get("consumer_id");
        if (consumerId == null) {
            consumerId = String.format(KAFKA_CONSUMER_ID_FMT, spec.name);
        }

        SpoutConfig config = new SpoutConfig(
            new ZkHosts(zkHosts,brokerZkPath),
            topic,
            zkRoot,
            consumerId
        );

        Boolean forceFromStart = (Boolean) spec.options.get("from_start");
        if (forceFromStart != null) {
            config.forceFromStart = forceFromStart;
        }

        Object startOffsetTime = spec.options.get("start_offset_time");
        if (startOffsetTime != null) {
            config.startOffsetTime = Long.valueOf(startOffsetTime.toString());
        }

        // TODO: this mandates that messages are UTF-8. We should allow for binary data
        // in the future, or once users can have Java components, let them provide their
        // own JSON serialization method. Or wait on STORM-138.
        config.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());

        return new KafkaSpout(config);
    }

    public static IRichSpout handlePythonSpout(
            @SuppressWarnings("unused") final TopologyBuilder builder,
            final SpoutSpec spec,
            final TopologySpec topologySpec) {

        PythonSpout spout = pyFactory.createPythonSpout(spec.module,
                spec.options, topologySpec.logging_config, topologySpec.serializer);

        if (spec.output_fields != null) {
            spout.setOutputFields(spec.output_fields);
        } else {
            throw new RuntimeException("Spouts must have output_fields");
        }

        if (spec.tick_freq_secs != -1) {
            spout.setTickFreqSecs(spec.tick_freq_secs);
        }

        return spout;
    }

    public static StormTopology buildTopology(final TopologySpec spec) {
        TopologyBuilder builder = new TopologyBuilder();

        for (final ComponentSpec component : spec.topology) {
            if (component.isBolt()) {
                handleBolt(builder, component.bolt, spec);
            } else if (component.isSpout()) {
                handleSpout(builder, component.spout, spec);
            } else {
                throw new RuntimeException(String.format("Unknown component: only bolts and spouts are supported."));
            }
        }

        return builder.createTopology();
    }

    private static InputStream getYamlInputStream(final String filename) throws FileNotFoundException {
        return PyleusTopologyBuilder.class.getResourceAsStream(filename);
    }

    private static void setSerializer(Config conf, final String serializer) {
        if (serializer.equals(TopologySpec.MSGPACK_SERIALIZER)) {
            conf.put(Config.TOPOLOGY_MULTILANG_SERIALIZER, MSGPACK_SERIALIZER_CLASS);
        } else if (serializer.equals(TopologySpec.JSON_SERIALIZER)) {
            // JSON_SERIALIZER is Storm default and nothing should be done
        } else {
            throw new RuntimeException(String.format("Unknown serializer: %s. Known: %s, %s",
                    serializer, TopologySpec.JSON_SERIALIZER, TopologySpec.MSGPACK_SERIALIZER));
        }
    }

    private static void runLocally(final String topologyName, final StormTopology topology, boolean debug, final String serializer) {
        Config conf = new Config();
        setSerializer(conf, serializer);
        conf.setDebug(debug);
        conf.setMaxTaskParallelism(1);

        final LocalCluster cluster = new LocalCluster();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    cluster.shutdown();
                } catch (Exception e) {
                    System.err.println(e.toString());
                }
            }
        });

        cluster.submitTopology(topologyName, conf, topology);

        // Sleep the main thread forever.
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        boolean runLocally = false;
        boolean debug = false;

        if (args.length > 2) {
            System.err.println("Usage: PyleusTopologyBuilder [--local [--debug]]");
            System.exit(1);
        }

        if (args.length == 1) {
            if (args[0].equals("--local")) {
                runLocally = true;
            } else {
                System.err.println("Usage: PyleusTopologyBuilder [--local [--debug]]");
                System.exit(1);
            }
        }

        if (args.length == 2) {
            if (args[0].equals("--local") && args[1].equals("--debug")) {
                runLocally = true;
                debug = true;
            } else {
                System.err.println("Usage: PyleusTopologyBuilder [--local [--debug]]");
                System.exit(1);
            }
        }

        final InputStream yamlInputStream;
        try {
            yamlInputStream = getYamlInputStream(YAML_FILENAME);
        } catch (final FileNotFoundException e) {
            System.err.println(String.format("File not found: %s", YAML_FILENAME));
            throw new RuntimeException(e);
        }

        final TopologySpec spec;
        try {
            spec = TopologySpec.create(yamlInputStream);
        } catch (final YAMLException e) {
            System.err.println(String.format("Unable to parse input file: %s", YAML_FILENAME));
            throw new RuntimeException(e);
        }

        StormTopology topology = buildTopology(spec);

        if (runLocally) {
            runLocally(spec.name, topology, debug, spec.serializer);
        } else {
            Config conf = new Config();
            conf.setDebug(false);

            setSerializer(conf, spec.serializer);

            if (spec.max_shellbolt_pending != -1) {
                conf.put(Config.TOPOLOGY_SHELLBOLT_MAX_PENDING, spec.max_shellbolt_pending);
            }

            if (spec.workers != -1) {
                conf.setNumWorkers(spec.workers);
            }

            if (spec.max_spout_pending != -1) {
                conf.setMaxSpoutPending(spec.max_spout_pending);
            }

            if (spec.message_timeout_secs != -1) {
                conf.setMessageTimeoutSecs(spec.message_timeout_secs);
            }

            if (spec.ackers != -1) {
                conf.setNumAckers(spec.ackers);
            }

            try {
                StormSubmitter.submitTopology(spec.name, conf, topology);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
