package com.tencent.trt.storm.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by wentao on 2/5/15.
 */
public class CoordinatorPeer extends Endpoint implements MessageHandler.Whole<String> {

    private final static Logger LOGGER = LoggerFactory.getLogger(CoordinatorPeer.class);
    // send to upstream to slow down
    public static final String COMMAND_BACKOFF = "backoff";
    // send to downstream to flush
    public static final String COMMAND_FLUSH = "flush";
    private Session session;
    private int backoffCounter = 1;
    private long ignoreBackoffUntil = 0;

    @Override
    public void onOpen(Session session, EndpointConfig endpointConfig) {
        this.session = session;
        session.addMessageHandler(this);
    }

    @Override
    public void onMessage(String msg) {

    }

    public void flush(int inSyncRecordTime) {
        HashMap command = new HashMap();
        command.put(TaskCommandEndpoint.KEY_COMMAND, COMMAND_FLUSH);
        command.put("in_sync_record_time", inSyncRecordTime);
        try {
            session.getAsyncRemote().sendText(new ObjectMapper().writeValueAsString(command));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int backoff() {
        if (System.currentTimeMillis() < ignoreBackoffUntil) {
            return 0;
        }
        backoffCounter++;
        backoffCounter = Math.min(backoffCounter, 6);
        HashMap command = new HashMap();
        command.put(TaskCommandEndpoint.KEY_COMMAND, COMMAND_BACKOFF);
        int seconds = (int) Math.pow(2, backoffCounter);
        command.put("seconds", seconds);
        try {
            session.getAsyncRemote().sendText(new ObjectMapper().writeValueAsString(command));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ignoreBackoffUntil = System.currentTimeMillis() + seconds * 1000;
        return seconds;
    }

    public void resetBackoffCounter() {
        backoffCounter = 1;
    }

    public void close() {
        try {
            session.close();
        } catch (Exception e) {
            LOGGER.debug("failed to close", e);
        }
    }
}
