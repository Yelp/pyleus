package com.yelp.pyleus.spec;

import java.util.List;
import java.util.Map;

public class BoltSpec {
    public String name;
    public String type;
    public String module;
    public Map<String, Object> options;
    public Map<String, Object> output_fields;
    public Float tick_freq_secs = -1.f;
    public Integer parallelism_hint = -1;
    public Integer tasks = -1;
    public List<Map<String, Object>> groupings;
}
