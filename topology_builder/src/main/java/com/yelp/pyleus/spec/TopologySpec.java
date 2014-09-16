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
    // overflow and do not imply better performances. We define the default value
    // as 1/10 the storm default max spout pending (1000). Lower values are suggested when the tuple size is very high.
    // Anyway, the pure java bolt does not buffer any tuple but simply processes a tuple at a time,
    // which is equivalent to set this value to 1.
    public static final Integer DEFAULT_MAX_SHELLBOLT_PENDING = 100;
	
    public String name;
    public List<ComponentSpec> topology;
    public Integer workers = -1;
    public Integer max_spout_pending = -1;
    public Integer message_timeout_secs = -1;
    public Integer ackers = -1;
    public Integer max_shellbolt_pending = DEFAULT_MAX_SHELLBOLT_PENDING;
    public String logging_config;
    public String requirements_filename; // Not used in Java.

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
