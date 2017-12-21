package org.folio.inventory.parsing;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.IOUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ModsParser {

  private final CharacterEncoding characterEncoding;

  public ModsParser(CharacterEncoding characterEncoding) {
    this.characterEncoding = characterEncoding;
  }

  public List<JsonObject> parseRecords(String xml)
    throws ParserConfigurationException,
    IOException,
    SAXException,
    XPathExpressionException {

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(false);
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(IOUtils.toInputStream(xml, "UTF-8"));

    XPathFactory xpathFactory = XPathFactory.newInstance();
    XPath xpath = xpathFactory.newXPath();

    ArrayList<JsonObject> parsedRecords = new ArrayList<>();

    NodeList records = (NodeList) xpath.compile("/mods_records/mods")
      .evaluate(doc, XPathConstants.NODESET);

    for(int recordIndex = 0; recordIndex < records.getLength(); recordIndex++) {
      JsonObject parsedRecord = new JsonObject();

      Node record = records.item(recordIndex);

      String title = (String)xpath.compile("titleInfo/title/text()")
        .evaluate(record, XPathConstants.STRING);

      String barcode = (String)xpath.compile("location/holdingExternal/localHolds/objId/text()")
        .evaluate(record, XPathConstants.STRING);

      JsonArray parsedIdentifiers = new JsonArray();

      NodeList recordIdentifiers = (NodeList)xpath.compile("recordInfo/recordIdentifier")
        .evaluate(record, XPathConstants.NODESET);

      for(int recordIdentifierIndex = 0; recordIdentifierIndex < recordIdentifiers.getLength(); recordIdentifierIndex++) {
        Node recordIdentifier = recordIdentifiers.item(recordIdentifierIndex);

        Node sourceAttribute = recordIdentifier.getAttributes().getNamedItem("source");

        String type = sourceAttribute != null ? sourceAttribute.getTextContent() : "";
        String value = recordIdentifier.getTextContent();

        parsedIdentifiers.add(new JsonObject()
          .put("type", type)
          .put("value", value));
      }

      NodeList identifiers = (NodeList)xpath.compile("identifier")
        .evaluate(record, XPathConstants.NODESET);

      for(int identifierIndex = 0; identifierIndex < identifiers.getLength(); identifierIndex++) {
        Node identifier = identifiers.item(identifierIndex);

        Node typeAttribute = identifier.getAttributes().getNamedItem("type");

        String type = typeAttribute != null ? typeAttribute.getTextContent() : "";
        String value = identifier.getTextContent();

        parsedIdentifiers.add(new JsonObject()
          .put("type", type)
          .put("value", value));
      }

      JsonArray parsedContributors = new JsonArray();

      NodeList contributors = (NodeList)xpath.compile("name/namePart[not(@type)]")
        .evaluate(record, XPathConstants.NODESET);

      for(int contributorIndex = 0; contributorIndex < contributors.getLength(); contributorIndex++) {
        Node contributor = contributors.item(contributorIndex);

        String name = contributor.getTextContent();

        parsedContributors.add(new JsonObject()
          .put("name", characterEncoding.decode(name)));
      }

      parsedRecord.put("title", characterEncoding.decode(title));
      parsedRecord.put("barcode", characterEncoding.decode(barcode));
      parsedRecord.put("identifiers", parsedIdentifiers);
      parsedRecord.put("contributors", parsedContributors);

      parsedRecords.add(parsedRecord);
    }

    return parsedRecords;
  }
}
