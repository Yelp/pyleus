package com.yelp.pyleus.spec;

import java.util.Map;

public class SpoutSpec {
    public String name;
    public String type;
    public String module;
    public Map<String, Object> options;
    public Map<String, Object> output_fields;
    public Float tick_freq_secs = -1.f;
    public Integer parallelism_hint = -1;
    public Integer tasks = -1;
}
