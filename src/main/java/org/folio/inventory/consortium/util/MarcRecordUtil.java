package org.folio.inventory.consortium.util;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayOutputStream;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Record;
import org.folio.inventory.dataimport.util.MarcContentCodec;
import org.folio.inventory.dataimport.util.MarcFieldEditor;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.marc4j.MarcException;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;

/**
 * Util to work with marc records
 */
public final class MarcRecordUtil {
  private static final Logger LOGGER = LogManager.getLogger();

  private MarcRecordUtil() { }

  /**
   * Removes subfields that contains values
   *
   * @param record       record that needs to be updated
   * @param fields       fields that could contain subfield
   * @param subfieldCode subfield to remove
   * @param values       values of the subfield to remove
   */
  public static void removeSubfieldsThatContainsValues(Record record, List<String> fields, char subfieldCode,
                                                     List<String> values) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
      MarcWriter marcStreamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
      MarcJsonWriter marcJsonWriter = new MarcJsonWriter(baos);
      org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
      if (marcRecord != null) {
        MarcFieldEditor.removeSubfieldValues(marcRecord, fields, subfieldCode, values);

        // use stream writer to recalculate leader
        marcStreamWriter.write(marcRecord);
        marcJsonWriter.write(marcRecord);

        String parsedContentString = new JsonObject(baos.toString()).encode();
        // save parsed content string to cache then set it on the record
        record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
      }
    }
  }

  /**
   * Removes all fields with the given tag from the marc record, recalculating the leader in the process.
   *
   * @param marcRecord record that needs to be updated
   * @param fieldTag   tag of the field(s) to remove
   * @return the same record instance, with its parsed record content updated if any field was removed
   */
  public static Record removeFieldFromMarcRecord(Record marcRecord, String fieldTag) {
    org.marc4j.marc.Record parsedMarcRecord = computeMarcRecord(marcRecord);
    if (parsedMarcRecord != null) {
      boolean fieldsRemoved = MarcFieldEditor.removeAllFieldsWithTag(parsedMarcRecord, fieldTag);
      if (fieldsRemoved) {
        try {
          String updatedContent = MarcContentCodec.serializeWithRecalculatedLeader(parsedMarcRecord);
          marcRecord.getParsedRecord().setContent(updatedContent);
        } catch (Exception e) {
          if (isOversizedRecordException(e)) {
            LOGGER.warn("removeFieldFromMarcRecord:: Record {} exceeds the MARC21 99999-byte length limit and "
              + "cannot be serialized", marcRecord.getId(), e);
          } else {
            LOGGER.warn("removeFieldFromMarcRecord:: Failed to remove field {} from record {}", fieldTag,
              marcRecord.getId(), e);
          }
        }
      }
    }
    return marcRecord;
  }

  /**
   * Detects marc4j's oversized-record failure - {@code MarcStreamWriter} refuses to write a record whose
   * ISO 2709 serialization would exceed the MARC21 99999-byte record-length limit.
   */
  private static boolean isOversizedRecordException(Exception e) {
    return e instanceof MarcException && e.getMessage() != null && e.getMessage().contains("99999 bytes");
  }

  /**
   * Check if any field with the subfield code exists.
   *
   * @param sourceRecord - source record.
   * @param subFieldCode - subfield code.
   * @return true if exists, otherwise false.
   */
  public static boolean isSubfieldExist(Record sourceRecord, char subFieldCode) {
    try {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(sourceRecord);
      if (marcRecord != null) {
        return MarcFieldEditor.subfieldExists(marcRecord, subFieldCode);
      }
    } catch (Exception e) {
      LOGGER.warn("isSubfieldExist:: Error during the search a subfield in the record", e);
      return false;
    }
    return false;
  }

  private static org.marc4j.marc.Record computeMarcRecord(Record record) {
    if (record != null
        && record.getParsedRecord() != null
        && isNotBlank(record.getParsedRecord().getContent().toString())) {
      try {
        var content = normalizeContent(record.getParsedRecord().getContent());
        return MarcContentCodec.parse(content).orElse(null);
      } catch (Exception e) {
        LOGGER.warn("computeMarcRecord:: Error during the transformation to marc record", e);
        try {
          String fallbackContent = ParsedRecordUtil.normalize(record.getParsedRecord().getContent()).encode();
          return MarcContentCodec.parse(fallbackContent).orElse(null);
        } catch (Exception ex) {
          LOGGER.warn("computeMarcRecord:: Error during the building of MarcReader", ex);
        }
        return null;
      }
    }
    return null;
  }

  private static String normalizeContent(Object o) {
    return (o instanceof String content)
           ? content
           : Json.encode(o);
  }
}
