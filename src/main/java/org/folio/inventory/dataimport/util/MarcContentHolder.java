package org.folio.inventory.dataimport.util;

/**
 * Tiny SPI that lets the shared parse -&gt; mutate -&gt; write-back logic in {@link MarcRecordEditor} operate on
 * any FOLIO record shape without depending on any specific FOLIO {@code Record} type.
 *
 * <p>{@link #getMarcContent()} returns {@link Object} (not {@link String}) because a parsed record's content can
 * hold either a {@link String} or a structured type such as {@code JsonObject}/{@code Map} - {@link MarcContentCodec#canonicalize}
 * already normalizes either shape. {@link #setMarcContent(String)} always receives the canonical, already-serialized
 * content string produced by {@link MarcContentCodec#serializeWithRecalculatedLeader} - the holder's job is just to
 * write it back onto whatever underlying record type it wraps.
 */
public interface MarcContentHolder {

  Object getMarcContent();

  void setMarcContent(String content);

  default String getRecordId() {
    return null;
  }
}
