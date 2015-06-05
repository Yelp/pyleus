package com.yelp.pyleus.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
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
import static org.msgpack.template.Templates.tList;

import org.msgpack.type.Value;

public class MessagePackSerializer implements ISerializer {
    public static Logger LOG = Logger.getLogger(MessagePackSerializer.class);
    private DataOutputStream processIn;
    private InputStream processOut;
    private MessagePack msgPack;
    private Template<Map<String,Value>> mapTmpl;
    private Template<List<Value>> listTmpl;

    @Override
    public void initialize(OutputStream processIn, InputStream processOut) {
        this.processIn = new DataOutputStream(processIn);
        this.processOut = processOut;
        this.msgPack = new MessagePack();
        this.mapTmpl = tMap(TString, TValue);
        this.listTmpl = tList(TValue);
    }

    @SuppressWarnings("unchecked")
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
        // Write the message to the pipe
        writeMessage(setupmsg);

        Map<String, Value> pidmsg = readMessage();
        Value pid = pidmsg.get("pid");
        return pid.asIntegerValue().getInt();
    }

    @Override
    public ShellMsg readShellMsg() throws IOException, NoOutputException {
        Map<String, Value> msg = readMessage();
        ShellMsg shellMsg = new ShellMsg();

        String command = msg.get("command").asRawValue().getString();
        shellMsg.setCommand(command);

        Object id = null;
        Value valueId = msg.get("id");
        /* Since spouts can use both numbers and strings as ids, while bolts
         * only use strings, the check during acking was failing. Turning
         * everything into strings solves the problem. The issue does not
         * exist with JSON, instead.*/
        if (valueId != null) {
            if (valueId.isIntegerValue()) {
                id = msg.get("id").asIntegerValue().toString();
            } else {
                id = valueId.asRawValue().getString();
            }
        }
        shellMsg.setId(id);

        Value log = msg.get("msg");
        if(log != null) {
            shellMsg.setMsg(log.asRawValue().getString());
        }

        if (command.equals("log")) {
            Value logLevelValue = msg.get("level");
            if (logLevelValue != null) {
                shellMsg.setLogLevel(logLevelValue.asIntegerValue().getInt());
            }
        }

        String stream = Utils.DEFAULT_STREAM_ID;
        Value streamValue = msg.get("stream");
        if (streamValue != null) {
            stream = streamValue.asRawValue().getString();
        }
        shellMsg.setStream(stream);

        Value taskValue = msg.get("task");
        if (taskValue != null) {
            shellMsg.setTask(taskValue.asIntegerValue().getLong());
        } else {
            shellMsg.setTask(0);
        }

        Value need_task_ids = msg.get("need_task_ids");
        if (need_task_ids == null || (need_task_ids).asBooleanValue().getBoolean()) {
            shellMsg.setNeedTaskIds(true);
        } else {
            shellMsg.setNeedTaskIds(false);
        }

        Value tupleValue = msg.get("tuple");
        if (tupleValue != null) {
            for (Value element:tupleValue.asArrayValue()) {
                /* Tuples need to be Kryo serializable, while some msgpack-java type
                 * are not. Registering a Kryo serializer for them is not trivial at all,
                 * given how this package works. Problematic types are ByteArray, String,
                 * Map and List. This change is needed for ByteArrays and Strings. Nested
                 * Lists and Maps are not supported.*/
                shellMsg.addTuple(valueToJavaType(element));
            }
        }

        Value anchorsValue = msg.get("anchors");
        if(anchorsValue != null) {
            for (Value v: anchorsValue.asArrayValue()) {
                shellMsg.addAnchor(v.asRawValue().getString());
            }
        }
        return shellMsg;
    }

    private Object valueToJavaType(Value element) {
        switch (element.getType()) {
            case RAW:
                return element.asRawValue().getString();
            case INTEGER:
                return element.asIntegerValue().getLong();
            case FLOAT:
                return element.asFloatValue().getDouble();
            case BOOLEAN:
                return element.asBooleanValue().getBoolean();
            case NIL:
                return null;
            case ARRAY:
                List<Object> elementList = new ArrayList<Object>();
                for (Value e:element.asArrayValue().getElementArray()) {
                    elementList.add(valueToJavaType(e));
                }
                return elementList;
            case MAP:
                Map<Object,Object> elementMap = new HashMap<Object,Object>();
                for (Map.Entry<Value, Value> v:element.asMapValue().entrySet()) {
                    elementMap.put(
                            valueToJavaType(v.getKey()),
                            valueToJavaType(v.getValue()));
                }
                return elementMap;
            default:
                return element;
        }
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
