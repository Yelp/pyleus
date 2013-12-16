package com.yelp.pyleus.spec;

import java.io.InputStream;
import java.util.List;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class TopologySpec {
    public String name;
    public List<ComponentSpec> topology;
    public Integer workers = -1;
    public Integer max_spout_pending = -1;
    public Integer message_timeout_secs = -1;

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
