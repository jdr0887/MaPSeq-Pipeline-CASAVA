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
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            // {"account_name":"rc_renci.svc","entities":[{"entity_type":"FileData","id":"15251"},{"entity_type":"WorkflowRun","name":"121204_UNC12-SN629_0241_BC180FACXX_CASAVA"}]}
            // {"account_name":"rc_renci.svc","entities":[{"entity_type":"FileData","id":"15255"},{"entity_type":"WorkflowRun","name":"121213_UNC14-SN744_0279_BD1MCVACXX_CASAVA"}]}
            // {"account_name":"rc_renci.svc","entities":[{"entity_type":"FileData","id":"51595"},{"entity_type":"WorkflowRun","name":"121212_UNC13-SN749_0207_BD1LCDACXX_CASAVA"}]}
            // {"account_name":"rc_renci.svc","entities":[{"entity_type":"FileData","id":"50683"},{"entity_type":"WorkflowRun","name":"130105_UNC15-SN850_0250_BC1JMKACXX_CASAVA"}]}
            // {"account_name":"rc_renci.svc","entities":[{"entity_type":"FileData","id":"10710"},{"entity_type":"WorkflowRun","name":"121109_UNC10-SN254_0391_BC0YNYACXX_CASAVA"}]}

            String format = "{\"account_name\":\"%s\",\"entities\":[{\"entity_type\":\"FileData\",\"id\":\"%d\"},{\"entity_type\":\"WorkflowRun\",\"name\":\"%s_CASAVA\"}]}";

            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 11064,
            // "121119_UNC12-SN629_0239_BC11DEACXX")));
            //
            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 11065,
            // "121119_UNC12-SN629_0238_AD12N7ACXX")));
            //
            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 11066,
            // "121114_UNC15-SN850_0241_AD16CYACXX")));
            //
            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 10710,
            // "121109_UNC10-SN254_0391_BC0YNYACXX")));

            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 15251,
            // "121204_UNC12-SN629_0241_BC180FACXX")));
            //
            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 15255,
            // "121213_UNC14-SN744_0279_BD1MCVACXX")));
            //
            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 51595,
            // "121212_UNC13-SN749_0207_BD1LCDACXX")));
            //
            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 50683,
            // "130105_UNC15-SN850_0250_BC1JMKACXX")));
            //
            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 10710,
            // "121109_UNC10-SN254_0391_BC0YNYACXX")));

            // producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 29107,
            // "130104_UNC16-SN851_0201_BC1GHTACXX")));

            producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 435964,
                    "131022_UNC16-SN851_0288_AH7FALADXX")));

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
