package com.yelp.pyleus.serializer;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import backtype.storm.multilang.BoltMsg;
import backtype.storm.multilang.ISerializer;
import backtype.storm.multilang.NoOutputException;
import backtype.storm.multilang.ShellMsg;
import backtype.storm.multilang.SpoutMsg;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import org.apache.log4j.Logger;
import org.msgpack.MessagePack;
import org.msgpack.template.Template;

import static org.msgpack.template.Templates.tMap;
import static org.msgpack.template.Templates.TString;
import static org.msgpack.template.Templates.TValue;

import org.msgpack.type.Value;

public class MessageSerializer implements ISerializer {
    public static Logger LOG = Logger.getLogger(MessageSerializer.class);
	private DataOutputStream processIn;
	private InputStream processOut;
	private MessagePack msgPack;
	private Template<Map<String,Value>> mapTmpl;

	@Override
	public void initialize(OutputStream processIn, InputStream processOut) {
		this.processIn = new DataOutputStream(processIn);
		this.processOut = processOut;
		this.msgPack = new MessagePack();
		this.mapTmpl = tMap(TString, TValue);
	}

	private Map<String, Object> getMapFromContext(TopologyContext context) {
		Map context_map = new HashMap();
		context_map.put("taskid", context.getThisTaskId());
		context_map.put("task->component", context.getTaskToComponent());
		return context_map;
	}

	@Override
	public Number connect(Map conf, TopologyContext context) throws IOException,
			NoOutputException {
		// Create the setup message for the initial handshake
		Map<String, Object> setupmsg = new HashMap<String, Object>();
		setupmsg.put("conf", conf);
        setupmsg.put("pidDir", context.getPIDDir());
		setupmsg.put("context", getMapFromContext(context));
		// write the message to the pipe
        LOG.info("Writing configuration to shell component");
		writeMessage(setupmsg);

        LOG.info("Waiting for pid from component");
		Map<String, Value> pidmsg = readMessage();
        LOG.info("Shell component connection established.");
		Value pid = pidmsg.get("pid");
		return (Number) pid.asIntegerValue().getInt();
	}


	@Override
	public ShellMsg readShellMsg() throws IOException, NoOutputException {
		Map<String, Value> msg = readMessage();
		ShellMsg shellMsg = new ShellMsg();

		String command = msg.get("commang").asRawValue().getString();
		shellMsg.setCommand(command);

        Object id = msg.get("id");
        shellMsg.setId(id);

        String log = msg.get("msg").asRawValue().getString();
        shellMsg.setMsg(log);

        String stream = msg.get("stream").asRawValue().getString();
        if (stream == null)
            stream = Utils.DEFAULT_STREAM_ID;
        shellMsg.setStream(stream);

        Object taskObj = msg.get("task");
        if (taskObj != null) {
            shellMsg.setTask((Long) taskObj);
        } else {
            shellMsg.setTask(0);
        }

        Value need_task_ids = msg.get("need_task_ids");
        if (need_task_ids == null || (need_task_ids).asBooleanValue().getBoolean()) {
            shellMsg.setNeedTaskIds(true);
        } else {
            shellMsg.setNeedTaskIds(false);
        }

        shellMsg.addTuple(msg.get("tuple").asArrayValue().getElementArray());


        List<Value> anchors = (Arrays.asList(msg.get("anchors").asArrayValue().getElementArray()));
        for (Value v: anchors) {
		shellMsg.addAnchor(v.asRawValue().getString());
        }

		return shellMsg;
	}

	@Override
	public void writeBoltMsg(BoltMsg boltMsg) throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("id", boltMsg.getId());
        map.put("comp", boltMsg.getComp());
        map.put("stream", boltMsg.getStream());
        map.put("task", boltMsg.getTask());
        map.put("tuple", boltMsg.getTuple());

        writeMessage(map);
	}

	@Override
	public void writeSpoutMsg(SpoutMsg spoutMsg) throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
        map.put("command", spoutMsg.getCommand());
        map.put("id", spoutMsg.getId());

        writeMessage(map);
	}

	@Override
	public void writeTaskIds(List<Integer> taskIds) throws IOException {
		writeMessage(taskIds);
	}

	private Map<String, Value> readMessage() throws IOException {
		return msgPack.read(this.processOut, this.mapTmpl);
	}

	private void writeMessage(List<Integer> msg) throws IOException{
		msgPack.write(this.processIn, msg);
		this.processIn.flush();
	}

	private void writeMessage(Map<String, Object> msg) throws IOException {
		msgPack.write(this.processIn, msg);
		this.processIn.flush();
	}
}
