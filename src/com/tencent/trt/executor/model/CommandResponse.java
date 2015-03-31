package com.tencent.trt.executor.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.websocket.Session;
import java.io.IOException;

/**
 * Created by wentao on 2/4/15.
 */
public abstract class CommandResponse {

    public abstract void stream(Session session);

    public static Text text(Object obj) {
        return new Text(obj);
    }

    public static Text text(String text) {
        return new Text(text);
    }

    public static class Text extends CommandResponse {

        private final String text;

        public Text(String text) {
            this.text = text;
        }

        public Text(Object obj) {
            try {
                this.text = new ObjectMapper().writeValueAsString(obj);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void stream(Session session) {
            try {
                session.getBasicRemote().sendText(text);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
