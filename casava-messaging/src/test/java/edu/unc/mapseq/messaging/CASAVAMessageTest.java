package edu.unc.mapseq.messaging;

import java.io.IOException;
import java.io.StringWriter;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class CASAVAMessageTest {

    @Test
    public void testCAVASAQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "152.19.198.146"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/casava");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            String format = "{\"entities\":[{\"entityType\":\"FileData\",\"id\":\"%d\"},{\"entityType\":\"WorkflowRun\",\"name\":\"%s_CASAVA\"}]}";
            // String format =
            // "{\"entities\":[{\"entityType\":\"FileData\",\"id\":\"%d\"},{\"entityType\":\"WorkflowRun\",\"name\":\"%s_CASAVA\"}]}";
            producer.send(session.createTextMessage(String.format(format, 459863, "140519_UNC16-SN851_0359_AH9GE5ADXX")));

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

    @Test
    public void testJSON() {

        try {
            StringWriter sw = new StringWriter();

            JsonGenerator generator = new JsonFactory().createGenerator(sw);

            generator.writeStartObject();
            generator.writeArrayFieldStart("entities");

            generator.writeStartObject();
            generator.writeStringField("entityType", "Sample");
            generator.writeStringField("guid", "12345");
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("entityType", "WorkflowRun");
            generator.writeStringField("name", "my workflow run");
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.flush();
            generator.close();

            sw.flush();
            sw.close();
            System.out.println(sw.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
