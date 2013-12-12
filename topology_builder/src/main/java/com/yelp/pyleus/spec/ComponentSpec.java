package com.yelp.pyleus.spec;

public class ComponentSpec {
    public BoltSpec bolt = null;
    public SpoutSpec spout = null;

    public boolean isBolt() {
        return bolt != null;
    }

    public boolean isSpout() {
        return spout != null;
    }
}
