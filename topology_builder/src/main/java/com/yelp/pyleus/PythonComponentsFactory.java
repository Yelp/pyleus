package com.yelp.pyleus;

import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.pyleus.bolt.PythonBolt;
import com.yelp.pyleus.spout.PythonSpout;

public class PythonComponentsFactory {

    public static final String VIRTUALENV_PATH = "/resources/pyleus_venv/bin/python";
    public static final String VIRTUALENV_INTERPRETER = "pyleus_venv/bin/python";
    public static final String MODULE_OPTION = "-m";

    private String[] buildCommand(final String module, final Map<String, Object> argumentsMap) {
        String[] command = new String[3];

        command[0] = "bash";
        command[1] = "-c";

        StringBuilder strBuf = new StringBuilder();
        // Done before launching any spout or bolt in order to cope with Storm permissions bug
        strBuf.append(String.format("chmod 755 %s; %s", VIRTUALENV_INTERPRETER, VIRTUALENV_INTERPRETER));
        strBuf.append(String.format(" %s %s", MODULE_OPTION, module));

        if (argumentsMap != null) {
            Gson gson = new GsonBuilder().create();
            String json = gson.toJson(argumentsMap);
            json = json.replace("\"", "\\\"");
            strBuf.append(String.format(" --options \"%s\"", json));
        }

        command[2] = strBuf.toString();

        return command;
    }

    public PythonBolt createPythonBolt(final String module, final Map<String, Object> argumentsMap) {
        return new PythonBolt(buildCommand(module, argumentsMap));
    }

    public PythonSpout createPythonSpout(final String module, final Map<String, Object> argumentsMap) {
        return new PythonSpout(buildCommand(module, argumentsMap));
    }

}
