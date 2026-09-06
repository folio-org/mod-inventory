package org.folio.inventory.dataimport.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.marc4j.MarcException;
import support.TestUtil;

class MarcContentCodecTest {

  private static final String PARSED_MARC_RECORD_PATH = "src/test/resources/marc/parsedMarcRecord.json";

  @DisplayName("should parse well-formed MARC-in-JSON content into a marc4j record")
  @Test
  void shouldParseValidContent_intoMarc4jRecord() {
    // arrange
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);

    // act
    Optional<org.marc4j.marc.Record> result = MarcContentCodec.parse(parsedRecordContent);

    // assert
    assertThat(result).isPresent();
    assertThat(MarcFieldEditor.getControlFieldValue(result.get(), "001")).isEqualTo("ybp7406411");
  }

  @DisplayName("should return an empty Optional when the reader produces no record for the given content")
  @Test
  void shouldReturnEmptyOptional_whenContentProducesNoRecord() {
    // act
    Optional<org.marc4j.marc.Record> result = MarcContentCodec.parse("");

    // assert
    assertThat(result).isEmpty();
  }

  @DisplayName("should propagate the parser's exception instead of swallowing it, when content is malformed JSON")
  @Test
  void shouldPropagateException_whenContentIsMalformedJson() {
    // arrange: marc4j's own hand-rolled JSON parser rejects this - hasNext() reports true (content is present)
    // but next() throws while actually reading it. parse() must let that exception through unchanged, rather
    // than converting it into an empty Optional - callers (e.g. AdditionalFieldsUtil.computeMarcRecord) rely on
    // a real exception here to trigger their own fallback parse attempt.
    String malformedContent = "{fields}";

    // act / assert
    assertThatThrownBy(() -> MarcContentCodec.parse(malformedContent))
      .isInstanceOf(RuntimeException.class);
  }

  @DisplayName("should recalculate the leader and reflect a mutation made to the parsed record")
  @Test
  void shouldRecalculateLeaderAndSerializeMutation_whenRecordIsMutatedAfterParsing() throws Exception {
    // arrange
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    String originalLeader = new JsonObject(parsedRecordContent).getString("leader");
    org.marc4j.marc.Record marcRecord = MarcContentCodec.parse(parsedRecordContent).orElseThrow();
    MarcFieldEditor.addOrReplaceControlField(marcRecord, "002", "test-value", false);

    // act
    String serializedContent = MarcContentCodec.serializeWithRecalculatedLeader(marcRecord);

    // assert
    JsonObject serialized = new JsonObject(serializedContent);
    assertThat(serialized.getString("leader")).isNotEqualTo(originalLeader);
    JsonArray fields = serialized.getJsonArray("fields");
    boolean has002Field = fields.stream()
      .map(JsonObject.class::cast)
      .anyMatch(field -> field.containsKey("002") && "test-value".equals(field.getString("002")));
    assertThat(has002Field).isTrue();
  }

  @DisplayName("should propagate marc4j's oversized-record MarcException instead of swallowing it")
  @Test
  void shouldPropagateOversizedRecordException_whenSerializedContentExceedsMarc21LengthLimit() throws Exception {
    // arrange: one field with a huge subfield value pushes the total ISO 2709 record length past marc4j's
    // MARC21 99999-byte ceiling. MarcStreamWriter throws MarcException past that limit; serializeWithRecalculatedLeader
    // must let it propagate unchanged so callers can keep distinguishing this specific failure from any other.
    String hugeValue = "a".repeat(150_000);
    String content = "{\"leader\":\"00000nam a2200000 a 4500\",\"fields\":[{\"001\":\"in001\"},"
                     + "{\"999\":{\"subfields\":[{\"a\":\"" + hugeValue + "\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    org.marc4j.marc.Record marcRecord = MarcContentCodec.parse(content).orElseThrow();

    // act / assert
    assertThatThrownBy(() -> MarcContentCodec.serializeWithRecalculatedLeader(marcRecord))
      .isInstanceOf(MarcException.class)
      .hasMessageContaining("99999 bytes");
  }

  @DisplayName("should canonicalize String content by round-tripping it through a JSON object")
  @Test
  void shouldCanonicalizeStringContent_intoCanonicalJsonForm() {
    // arrange
    String content = "{\"leader\":\"01234\",   \"fields\":[{\"001\":\"abc\"}]}";

    // act
    String canonical = MarcContentCodec.canonicalize(content);

    // assert
    assertThat(canonical).isEqualTo(new JsonObject(content).encode());
  }

  @DisplayName("should canonicalize Map content the same way JsonObject.mapFrom would")
  @Test
  void shouldCanonicalizeMapContent_sameAsJsonObjectMapFrom() {
    // arrange
    Map<String, Object> content = Map.of("leader", "01234", "fields", List.of(Map.of("001", "abc")));

    // act
    String canonical = MarcContentCodec.canonicalize(content);

    // assert
    assertThat(canonical).isEqualTo(JsonObject.mapFrom(content).encode());
  }

  @DisplayName("should canonicalize JsonObject content the same way JsonObject.mapFrom would")
  @Test
  void shouldCanonicalizeJsonObjectContent_sameAsJsonObjectMapFrom() {
    // arrange
    JsonObject content = new JsonObject().put("leader", "01234");

    // act
    String canonical = MarcContentCodec.canonicalize(content);

    // assert
    assertThat(canonical).isEqualTo(JsonObject.mapFrom(content).encode());
  }
}
