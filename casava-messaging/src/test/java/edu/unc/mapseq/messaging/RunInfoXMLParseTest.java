package edu.unc.mapseq.messaging;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class RunInfoXMLParseTest {

    @Test
    public void testParseRunInfo() {
        try {
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputStream is = this.getClass().getResourceAsStream("RunInfo.xml");
            InputSource inputSource = new InputSource(is);
            Document document = documentBuilder.parse(inputSource);
            XPath xpath = XPathFactory.newInstance().newXPath();

            // find the flowcell
            String runFlowcellIdPath = "/RunInfo/Run/Flowcell";
            Node runFlowcellIdNode = (Node) xpath.evaluate(runFlowcellIdPath, document, XPathConstants.NODE);
            String flowcell = runFlowcellIdNode.getTextContent();
            assertTrue("D0J7WACXX".equals(flowcell));

            int count = 0;
            String readsPath = "/RunInfo/Run/Reads/Read/@IsIndexedRead";
            NodeList readsNodeList = (NodeList) xpath.evaluate(readsPath, document, XPathConstants.NODESET);
            for (int index = 0; index < readsNodeList.getLength(); index++) {
                if ("N".equals(readsNodeList.item(index).getTextContent())) {
                    ++count;
                }
            }
            assertTrue(count == 2);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        } catch (DOMException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void parseDemultiplexStatsFile() {

        try {
            File statsFile = new File("/home/jdr0887/Downloads/Demultiplex_Stats.htm");
            org.jsoup.nodes.Document doc = Jsoup.parse(FileUtils.readFileToString(statsFile));
            Iterator<Element> tableIter = doc.select("table").iterator();
            tableIter.next();
            for (Element row : tableIter.next().select("tr")) {
                Elements tds = row.select("td");
                System.out.println(tds.text());
                System.out.println(tds.get(0).text());
                System.out.println(tds.get(1).text());
                System.out.println(tds.get(2).text());
                System.out.println(tds.get(3).text());
                System.out.println(tds.get(4).text());
                System.out.println(tds.get(5).text());
                System.out.println(tds.get(6).text());
                System.out.println(tds.get(7).text());
                System.out.println(tds.get(8).text());
                System.out.println(tds.get(9).text());
                System.out.println(tds.get(10).text());
                System.out.println(tds.get(11).text());
                System.out.println(tds.get(12).text());
                System.out.println(tds.get(13).text());
                System.out.println(tds.get(14).text());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
