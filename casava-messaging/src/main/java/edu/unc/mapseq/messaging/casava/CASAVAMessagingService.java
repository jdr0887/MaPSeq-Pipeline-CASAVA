package edu.unc.mapseq.messaging.casava;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.pipeline.PipelineBeanService;

public class CASAVAMessagingService {

    private final Logger logger = LoggerFactory.getLogger(CASAVAMessagingService.class);

    private ConnectionFactory connectionFactory;

    private PipelineBeanService pipelineBeanService;

    private String destinationName;

    private Connection connection;

    private Session session;

    public CASAVAMessagingService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");

        this.connection = connectionFactory.createConnection();
        this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = this.session.createQueue(this.destinationName);
        MessageConsumer consumer = this.session.createConsumer(destination);
        CASAVAMessageListener messageListener = new CASAVAMessageListener();
        messageListener.setPipelineBeanService(pipelineBeanService);
        consumer.setMessageListener(messageListener);

        this.connection.start();
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        if (this.session != null) {
            this.session.close();
        }
        if (this.connection != null) {
            this.connection.stop();
            this.connection.close();
        }
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public PipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(PipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

}
