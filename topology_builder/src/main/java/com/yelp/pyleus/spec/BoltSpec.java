package com.yelp.pyleus.spec;

import java.util.List;
import java.util.Map;

public class BoltSpec {
    public String name;
    public String module;
    public Map<String, Object> options;
    public List<String> output_fields;
    public Integer tick_freq_secs = -1;
    public Integer parallelism_hint = -1;
    public Integer tasks = -1;
    public List<Map<String, Object>> groupings;
}
