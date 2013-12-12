package com.yelp.pyleus.spout;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class PythonSpout extends ShellSpout implements IRichSpout {
    protected List<String> outputFields;
    protected Integer tickFreqSecs = null;

    public PythonSpout(final String... command) {
        super(command);
    }

    public void setOutputFields(final List<String> outputFields) {
        this.outputFields = outputFields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        assert this.outputFields != null; // Spouts must have output fields
        String[] array = this.outputFields.toArray(new String[this.outputFields.size()]);
        declarer.declare(new Fields(array));
    }

    public void setTickFreqSecs(Integer tickFreqSecs) {
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
