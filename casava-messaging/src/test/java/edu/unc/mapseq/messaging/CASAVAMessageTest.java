package edu.unc.mapseq.messaging;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

public class CASAVAMessageTest {

    @Test
    public void testCAVASAQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "biodev2.its.unc.edu"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/casava");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            String format = "{\"account_name\":\"%s\",\"entities\":[{\"entity_type\":\"FileData\",\"id\":\"%d\"},{\"entity_type\":\"WorkflowRun\",\"name\":\"%s_CASAVA\"}]}";

            producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 11064,
                    "121119_UNC12-SN629_0239_BC11DEACXX")));

            producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 11065,
                    "121119_UNC12-SN629_0238_AD12N7ACXX")));

            producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 11066,
                    "121114_UNC15-SN850_0241_AD16CYACXX")));

            producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 10710,
                    "121109_UNC10-SN254_0391_BC0YNYACXX")));

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }
}
