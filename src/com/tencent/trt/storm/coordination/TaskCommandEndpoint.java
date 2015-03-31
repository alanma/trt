package com.tencent.trt.storm.coordination;

import backtype.storm.task.TopologyContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.CommandHandler;
import com.tencent.trt.executor.model.CommandResponse;
import com.tencent.trt.storm.LoggingContext;
import com.tencent.trt.utils.JsonUtils;
import org.glassfish.tyrus.container.grizzly.server.GrizzlyServerContainer;
import org.glassfish.tyrus.server.TyrusServerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.*;
import javax.websocket.server.ServerEndpointConfig;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

/**
 * Created by wentao on 1/20/15.
 */
public class TaskCommandEndpoint extends Endpoint {

    private final static Logger LOGGER = LoggerFactory.getLogger(TaskCommandEndpoint.class);
    public static final String KEY_COMMAND = "command";
    private final String taskKey;
    private final CommandHandler commandHandler;
    private ObjectMapper objectMapper = new ObjectMapper();

    public TaskCommandEndpoint(String taskKey, CommandHandler commandHandler) {
        this.taskKey = taskKey;
        this.commandHandler = commandHandler;
    }

    public void handle(String message, Session session) {
        Map<String, Object> commandArgs = JsonUtils.readMap(objectMapper, message);
        String command = (String) commandArgs.remove(KEY_COMMAND);
        LOGGER.info(taskKey + " received " + command + " with args: " + commandArgs);
        RemoteEndpoint.Basic remote = session.getBasicRemote();
        try {
            remote.sendText("begin:" + command);
            remote.flushBatch();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        CommandResponse commandResponse = commandHandler.handleCommand(command, commandArgs);
        if (null != commandResponse) {
            commandResponse.stream(session);
        }
        try {
            remote.sendText("end:" + command);
            remote.flushBatch();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onOpen(final Session session, EndpointConfig endpointConfig) {
        session.addMessageHandler(new MessageHandler.Whole<String>() {
            @Override
            public void onMessage(String msg) {
                try {
                    handle(msg, session);
                } catch (Exception e) {
                    LOGGER.error("failed to handle msg: " + msg, e);
                }
            }
        });
    }

    public static String start(CommandHandler commandHandler, Map conf, TopologyContext context) {
        String localIp = LoggingContext.getLocalIp(conf);
        return start(commandHandler, context.getThisComponentId(), context.getThisTaskId(), localIp);
    }

    public static String start(
            final CommandHandler commandHandler,
            String componentId, int taskId, String localIp) {
        int port = getFreePort();
        final String taskKey = getTaskKey(componentId, taskId);
        TyrusServerContainer serverContainer = (TyrusServerContainer) GrizzlyServerContainer.createServerContainer();
        try {
            ServerEndpointConfig endpointConfig = ServerEndpointConfig.Builder
                    .create(TaskCommandEndpoint.class, "/command")
                    .configurator(new ServerEndpointConfig.Configurator() {
                        @Override
                        public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException {
                            return (T) new TaskCommandEndpoint(taskKey, commandHandler);
                        }
                    })
                    .build();
            serverContainer.start("/", port);
            serverContainer.register(endpointConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return localIp + ":" + port;
    }

    private static int getFreePort() {
        try {
            ServerSocket serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(null);
            int freePort = serverSocket.getLocalPort();
            serverSocket.close();
            return freePort;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getTaskKey(String componentId, Integer taskId) {
        return componentId + "[" + taskId + "]";
    }
}
