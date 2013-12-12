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
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.yelp.pyleus.bolt.PythonBolt;
import com.yelp.pyleus.spec.BoltSpec;
import com.yelp.pyleus.spec.ComponentSpec;
import com.yelp.pyleus.spec.SpoutSpec;
import com.yelp.pyleus.spec.TopologySpec;
import com.yelp.pyleus.spout.PythonSpout;

public class PyleusTopologyBuilder {

    public static final String YAML_FILENAME = "/resources/pyleus_topology.yaml";

    public static final PythonComponentsFactory pyFactory = new PythonComponentsFactory();

    public static void handleBolt(final TopologyBuilder builder, final BoltSpec bolt) {

        PythonBolt pythonBolt = pyFactory.createPythonBolt((String) bolt.module, (Map<String, Object>) bolt.options);

        if (bolt.output_fields != null) {
            pythonBolt.setOutputFields(bolt.output_fields);
        }

        if (bolt.tick_freq_secs != null) {
            pythonBolt.setTickFreqSecs(bolt.tick_freq_secs);
        }

        IRichBolt stormBolt = pythonBolt;

        BoltDeclarer declarer = null;
        if (bolt.parallelism_hint != null) {
            declarer = builder.setBolt(bolt.name, stormBolt, bolt.parallelism_hint);
        } else {
            declarer = builder.setBolt(bolt.name, stormBolt);
        }

        for (Map<String, Object> grouping : bolt.groupings) {
            assert grouping.size() == 1;
            Map.Entry<String, Object> entry = grouping.entrySet().iterator().next();
            String groupingType = entry.getKey();

            if (groupingType.equals("shuffle_grouping")) {
                String groupingTarget = (String) entry.getValue();
                declarer.shuffleGrouping(groupingTarget);
            } else if (groupingType.equals("fields_grouping")) {
                Map<String, Object> groupingOptions = (Map<String, Object>) entry.getValue();
                String groupingTarget = (String) groupingOptions.get("component");
                List<String> fields = (List<String>) groupingOptions.get("fields");

                String[] fieldsArray = fields.toArray(new String[fields.size()]);
                declarer.fieldsGrouping(groupingTarget, new Fields(fieldsArray));
            } else if (groupingType.equals("global_grouping")) {
                String groupingTarget = (String) entry.getValue();
                declarer.globalGrouping(groupingTarget);
            } else {
                throw new RuntimeException(String.format("Unknown grouping type: %s", groupingType));
            }
        }
    }

    public static void handleSpout(final TopologyBuilder builder, final SpoutSpec spout) {

        PythonSpout pythonSpout = pyFactory.createPythonSpout((String) spout.module, (Map<String, Object>) spout.options);

        if (spout.output_fields != null) {
            pythonSpout.setOutputFields(spout.output_fields);
        } else {
            throw new RuntimeException(String.format("Spouts must have output_fields"));
        }

        if (spout.tick_freq_secs != null) {
            pythonSpout.setTickFreqSecs(spout.tick_freq_secs);
        }

        IRichSpout stormSpout = pythonSpout;
        if (spout.parallelism_hint != null) {
            builder.setSpout(spout.name, stormSpout, spout.parallelism_hint);
        } else {
            builder.setSpout(spout.name, stormSpout);
        }
    }

    public static StormTopology buildTopology(final TopologySpec spec) {
        TopologyBuilder builder = new TopologyBuilder();

        for (final ComponentSpec component : spec.topology) {
            if (component.isBolt()) {
                handleBolt(builder, component.bolt);
            } else if (component.isSpout()) {
                handleSpout(builder, component.spout);
            } else {
                throw new RuntimeException(String.format("Unknown component: only bolts and spouts are supported."));
            }
        }

        return builder.createTopology();
    }

    private static InputStream getYamlInputStream(final String filename) throws FileNotFoundException {
        return PyleusTopologyBuilder.class.getResourceAsStream(filename);
    }

    private static void demo(final String topologyName, final StormTopology topology) {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);

        Utils.sleep(10 * 60 * 1000);

        cluster.killTopology(topologyName);
        cluster.shutdown();
    }

    public static void main(String[] args) {
        boolean runLocally = false;

        if (args.length > 1) {
            System.err.println("Usage: PyleusTopologyBuilder [--local]");
            System.exit(1);
        }

        if (args.length == 1) {
            if (args[0].equals("--local")) {
                runLocally = true;
            } else {
                System.err.println("Usage: PyleusTopologyBuilder [--local]");
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
            demo(spec.name, topology);
        } else {
            Config conf = new Config();
            conf.setDebug(false);
            conf.setNumWorkers(100);
            conf.setMaxSpoutPending(1000);

            try {
                StormSubmitter.submitTopology(spec.name, conf, topology);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
