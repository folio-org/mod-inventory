package org.folio.inventory.consortium.util;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.Record;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.VariableField;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Util to work with marc records
 */
public final class MarcRecordUtil {
  private static final Logger LOGGER = LogManager.getLogger();

  private MarcRecordUtil() {}

  /**
   * Removes subfields that contains values
   *
   * @param record   record that needs to be updated
   * @param fields    fields that could contain subfield
   * @param subfieldCode subfield to remove
   * @param values    values of the subfield to remove
   */
  public static void removeSubfieldsThatContainsValues(Record record, List<String> fields, char subfieldCode, List<String> values) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
      MarcWriter marcStreamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
      MarcJsonWriter marcJsonWriter = new MarcJsonWriter(baos);
      org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
      if (marcRecord != null) {
        for (VariableField variableField : marcRecord.getVariableFields(fields.toArray(new String[0]))) {
          DataField dataField = (DataField) variableField;
          List<Subfield> subfields = dataField.getSubfields(subfieldCode);
          for (Subfield subfield : subfields) {
            if (subfield != null && values.contains(subfield.getData())) {
              dataField.removeSubfield(subfield);
            }
          }
        }

        // use stream writer to recalculate leader
        marcStreamWriter.write(marcRecord);
        marcJsonWriter.write(marcRecord);

        String parsedContentString = new JsonObject(baos.toString()).encode();
        // save parsed content string to cache then set it on the record
        record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
      }
    }
  }

  private static org.marc4j.marc.Record computeMarcRecord(Record record) {
    if (record != null
      && record.getParsedRecord() != null
      && isNotBlank(record.getParsedRecord().getContent().toString())) {
      try {
        var content = normalizeContent(record.getParsedRecord().getContent());
        return getMarcRecordFromParsedContent(content);
      } catch (Exception e) {
        LOGGER.warn("computeMarcRecord:: Error during the transformation to marc record", e);
        try {
          MarcReader reader = buildMarcReader(record);
          if (reader.hasNext()) {
            return reader.next();
          }
        } catch (Exception ex) {
          LOGGER.warn("computeMarcRecord:: Error during the building of MarcReader", ex);
        }
        return null;
      }
    }
    return null;
  }

  private static org.marc4j.marc.Record getMarcRecordFromParsedContent(String parsedRecordContent) {
    MarcJsonReader marcJsonReader = new MarcJsonReader(new ByteArrayInputStream(parsedRecordContent.getBytes(StandardCharsets.UTF_8)));
    if (marcJsonReader.hasNext()) {
      return marcJsonReader.next();
    }
    return null;
  }

  private static MarcReader buildMarcReader(Record record) {
    String content = ParsedRecordUtil.normalize(record.getParsedRecord()).encode();
    return new MarcJsonReader(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
  }

  private static String normalizeContent(Object o) {
    return (o instanceof String content)
      ? content
      : Json.encode(o);
  }
}
