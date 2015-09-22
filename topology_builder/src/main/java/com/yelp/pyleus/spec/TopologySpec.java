package com.yelp.pyleus.spec;

import java.io.InputStream;
import java.util.List;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class TopologySpec {

    // This constant sets the default size of the pending queue for python ShellBolt.
    // If a python bolt cannot keep up with the incoming tuple stream, the pending queue will fill up
    // until reaching this value. Higher values of max shellbolt pending increase the risk of heap
    // exhaustion and do not imply better performances. The default value is 1, similarly to any java bolt,
    // which executes a tuple at a time. Tests and benchmarks did not show any worsening in performance
    // setting this value to 1.
    public static final Integer DEFAULT_MAX_SHELLBOLT_PENDING = 1;

    public static final String JSON_SERIALIZER = "json";
    public static final String MSGPACK_SERIALIZER = "msgpack";

    public String name;
    public List<ComponentSpec> topology;
    public Boolean topology_debug = false;
    public Integer workers = -1;
    public Integer max_spout_pending = -1;
    public Integer message_timeout_secs = -1;
    public Integer ackers = -1;
    public Integer max_shellbolt_pending = DEFAULT_MAX_SHELLBOLT_PENDING;

    public Integer sleep_spout_wait_strategy_time_ms = -1;
    public String worker_childopts_xmx = "";
    public Integer executor_receive_buffer_size = -1;
    public Integer executor_send_buffer_size = -1;
    public Integer transfer_buffer_size = -1;

    public String serializer = MSGPACK_SERIALIZER;
    public String logging_config;
    @SuppressWarnings("unused")
    public String requirements_filename; // Not used in Java.
    @SuppressWarnings("unused")
    public String python_interpreter; // Not used in Java.

    private static Constructor getConstructor() {
        Constructor constructor = new Constructor(TopologySpec.class);

        TypeDescription topologySpecDescription = new TypeDescription(TopologySpec.class);
        topologySpecDescription.putListPropertyType("topology", ComponentSpec.class);
        constructor.addTypeDescription(topologySpecDescription);

        TypeDescription componentSpecDescription = new TypeDescription(ComponentSpec.class);
        componentSpecDescription.putMapPropertyType("spout", String.class, SpoutSpec.class);
        componentSpecDescription.putMapPropertyType("bolt", String.class, BoltSpec.class);
        constructor.addTypeDescription(componentSpecDescription);

        return constructor;
    }

    public static TopologySpec create(final InputStream inputStream) {
        Yaml yaml = new Yaml(getConstructor());
        TopologySpec spec = (TopologySpec) yaml.load(inputStream);
        return spec;
    }
}
