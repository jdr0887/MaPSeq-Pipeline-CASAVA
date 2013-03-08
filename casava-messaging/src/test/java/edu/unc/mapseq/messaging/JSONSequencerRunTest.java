package edu.unc.mapseq.messaging;

import static org.junit.Assert.assertTrue;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

public class JSONSequencerRunTest {

    @Test
    public void parseJSONXStream() {
        // String value = "{\"sequencerRun\":{\"id\":\"1\"}}";

        // String value = "{"
        // + "\"account_name\":\"jreilly\","
        // + "\"entities\":[ "
        // +
        // "{\"guid\":\"1\", \"entity_type\": \"Sequencer run\", \"attributes\":[{\"name\":\"test.asdf\",\"value\":\"1\"}, {\"name\":\"test.qwer\",\"value\":\"2\"}]},"
        // + "{\"guid\":\"2\", \"entity_type\": \"Workflow run\" }]}";

        // String value = "{"
        // + "\"account_name\":\"jreilly\","
        // + "\"entities\":[ "
        // +
        // "{\"guid\":\"1\", \"entity_type\": \"Sequencer run\", \"attributes\":[{\"name\":\"GATKDepthOfCoverage.interval_list.version\",\"value\":\"1\"}, {\"name\":\"SAMToolsView.dx.id\",\"value\":\"2\"}]},"
        // +
        // "{\"workflowName\":\"NCGenes\",\"workflowRunName\":\"my-first-run\", \"entity_type\": \"Workflow run\" }]}";

        String value = "{"
                + "\"account_name\":\"jreilly\","
                + "\"entities\":[ "
                + "{\"entity_type\": \"Sequencer run\", \"guid\":\"1\", \"attributes\":[{\"name\":\"GATKDepthOfCoverage.interval_list.version\",\"value\":\"1\"}, {\"name\":\"SAMToolsView.dx.id\",\"value\":\"2\"}]},"
                + "{\"entity_type\": \"Workflow run\", \"name\":\"my-run-1\"}]}";

        try {

            JSONObject jsonObject = new JSONObject(value);
            assertTrue(jsonObject.has("account_name"));
            assertTrue(jsonObject.has("entities"));
            assertTrue(jsonObject.getString("account_name").equals("jreilly"));
            JSONArray entityArray = jsonObject.getJSONArray("entities");
            assertTrue(entityArray.length() == 2);

            JSONObject jo = entityArray.getJSONObject(0);
            String entityType = jo.getString("entity_type");
            assertTrue("Sequencer run".equals(entityType));
            Long guid = jo.getLong("guid");
            assertTrue(1L == guid);

            if (jo.has("attributes")) {
                JSONArray attributeArray = jo.getJSONArray("attributes");
                JSONObject att = attributeArray.getJSONObject(0);
                String attributeName = att.getString("name");
                assertTrue(attributeName.equals("GATKDepthOfCoverage.interval_list.version"));
                String attributeValue = att.getString("value");
                assertTrue(attributeValue.equals("1"));
                //
                // for (int j = 0; j < attributeArray.length(); ++j) {
                // JSONObject att = attributeArray.getJSONObject(j);
                // String attributeName = att.getString("name");
                // assertTrue(attributeName.equals("GATKDepthOfCoverage.interval_list.version"));
                // String attributeValue = att.getString("value");
                // assertTrue(attributeValue.equals("1"));
                // }
            }

            JSONArray attributeArray = jo.getJSONArray("attributes");
            assertTrue(attributeArray.length() == 2);

            jo = entityArray.getJSONObject(1);
            entityType = jo.getString("entity_type");
            assertTrue("Workflow run".equals(entityType));
            // guid = jo.getLong("guid");
            // assertTrue(2L == guid);

            // Long id = sequencerRun.getId();
            // assertTrue(1L == id);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }

    }

    @Test
    public void parseJSONJettison() {
        String value = "{\"guid\":\"1\"}";
        try {
            JSONObject jsonObject = new JSONObject(value);
            Long id = jsonObject.getLong("guid");
            assertTrue(1L == id);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
