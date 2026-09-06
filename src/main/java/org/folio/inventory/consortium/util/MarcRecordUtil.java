package org.folio.inventory.consortium.util;

import java.util.List;
import org.folio.Record;
import org.folio.inventory.dataimport.util.FolioRecordHolder;
import org.folio.inventory.dataimport.util.MarcRecordEditor;

/**
 * Util to work with marc records.
 *
 * <p>Pure facade over {@link MarcRecordEditor} (the shared parse -&gt; mutate -&gt; write-back logic, including
 * the parsed-record-content cache) via a {@link FolioRecordHolder} adapter - no independent cache, parsing, or
 * field-manipulation logic of its own.
 */
public final class MarcRecordUtil {

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
    MarcRecordEditor.removeSubfieldsThatContainsValues(new FolioRecordHolder(record), fields, subfieldCode, values);
  }

  /**
   * Removes all fields with the given tag from the marc record, recalculating the leader in the process.
   *
   * @param marcRecord record that needs to be updated
   * @param fieldTag   tag of the field(s) to remove
   * @return the same record instance, with its parsed record content updated if any field was removed
   */
  public static Record removeFieldFromMarcRecord(Record marcRecord, String fieldTag) {
    MarcRecordEditor.removeFieldFromMarcRecord(new FolioRecordHolder(marcRecord), fieldTag);
    return marcRecord;
  }

  /**
   * Check if any field with the subfield code exists.
   *
   * @param sourceRecord - source record.
   * @param subFieldCode - subfield code.
   * @return true if exists, otherwise false.
   */
  public static boolean isSubfieldExist(Record sourceRecord, char subFieldCode) {
    return MarcRecordEditor.isSubfieldExist(new FolioRecordHolder(sourceRecord), subFieldCode);
  }
}
