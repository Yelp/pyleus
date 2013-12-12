package com.yelp.pyleus.spec;

import java.util.List;
import java.util.Map;

public class BoltSpec {
    public String name;
    public String module;
    public Map<String, Object> options;
    public List<String> output_fields;
    public Integer tick_freq_secs;
    public Integer parallelism_hint;
    public List<Map<String, Object>> groupings;
}
