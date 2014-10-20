package com.yelp.pyleus.spout;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.Config;
import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class PythonSpout extends ShellSpout implements IRichSpout {
    protected Map<String, Object> outputFields;
    protected Float tickFreqSecs = null;

    public PythonSpout(final String... command) {
        super(command);
    }

    public void setOutputFields(final Map<String, Object> outputFields) {
        this.outputFields = outputFields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(Entry<String, Object> outEntry : this.outputFields.entrySet()) {
            String stream = outEntry.getKey();
            @SuppressWarnings("unchecked")
            List<String> fields = (List<String>) outEntry.getValue();
            declarer.declareStream(stream, new Fields(fields.toArray(new String[fields.size()])));
        }
    }

    public void setTickFreqSecs(Float tickFreqSecs) {
        this.tickFreqSecs = tickFreqSecs;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if (this.tickFreqSecs == null) {
            return null;
        }

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, this.tickFreqSecs);
        return conf;
    }
}
