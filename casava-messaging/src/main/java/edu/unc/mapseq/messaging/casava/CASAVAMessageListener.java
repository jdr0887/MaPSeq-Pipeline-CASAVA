package edu.unc.mapseq.messaging.casava;

import java.util.concurrent.Executors;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.pipeline.casava.CasavaPipelineBeanService;

public class CASAVAMessageListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(CASAVAMessageListener.class);

    private CasavaPipelineBeanService pipelineBeanService;

    public CASAVAMessageListener() {
        super();
    }

    public CASAVAMessageListener(CasavaPipelineBeanService pipelineBeanService) {
        super();
        this.pipelineBeanService = pipelineBeanService;
    }

    @Override
    public void onMessage(Message message) {
        logger.debug("ENTERING onMessage(Message)");

        String messageValue = null;

        try {
            if (message instanceof TextMessage) {
                logger.debug("received TextMessage");
                TextMessage textMessage = (TextMessage) message;
                messageValue = textMessage.getText();
            }
        } catch (JMSException e2) {
            e2.printStackTrace();
        }

        if (StringUtils.isEmpty(messageValue)) {
            logger.warn("message value is empty");
            return;
        }

        logger.info("messageValue: {}", messageValue);

        JSONObject jsonMessage = null;

        try {
            jsonMessage = new JSONObject(messageValue);
            if (!jsonMessage.has("entities") || !jsonMessage.has("account_name")) {
                logger.error("json lacks entities or account_name");
                return;
            }
        } catch (JSONException e) {
            logger.error("BAD JSON format", e);
            return;
        }

        CASAVAMessageRunnable runnable = new CASAVAMessageRunnable();
        runnable.setJsonMessage(jsonMessage);
        runnable.setPipelineBeanService(pipelineBeanService);
        Executors.newSingleThreadExecutor().submit(runnable);
    }

    public CasavaPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(CasavaPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
