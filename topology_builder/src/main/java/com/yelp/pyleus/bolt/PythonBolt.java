package com.yelp.pyleus.bolt;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class PythonBolt extends ShellBolt implements IRichBolt {
    protected List<String> outputFields;
    protected Integer tickFreqSecs = null;

    public PythonBolt(final String... command) {
        super(command);
    }

    public void setOutputFields(final List<String> outputFields) {
        this.outputFields = outputFields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.outputFields != null) {
            String[] array = this.outputFields.toArray(new String[this.outputFields.size()]);
            declarer.declare(new Fields(array));
        } else {
            declarer.declare(new Fields());
        }
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
