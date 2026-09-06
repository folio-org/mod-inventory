package org.folio.inventory.dataimport.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MarcJsonFieldOrdererTest {

  private static final String PARSED_RECORD = "src/test/resources/marc/parsedRecord.json";
  private static final String REORDERED_PARSED_RECORD = "src/test/resources/marc/reorderedParsedRecord.json";
  private static final String REORDERING_RESULT_RECORD = "src/test/resources/marc/reorderingResultRecord.json";

  @DisplayName("should reorder system field order to match incoming record field order, pinning the same "
    + "fixtures and expectation as AdditionalFieldsUtilTest's reorder coverage")
  @Test
  void shouldReorderFields_whenBothContentsAreWellFormed() throws IOException {
    // arrange
    var sourceOrderContent = readFileFromPath(REORDERED_PARSED_RECORD);
    var systemOrderContent = readFileFromPath(PARSED_RECORD);
    var expectedContent = readFileFromPath(REORDERING_RESULT_RECORD);

    // act
    var actualContent = MarcJsonFieldOrderer.reorderFields(sourceOrderContent, systemOrderContent);

    // assert
    assertThat(formatContent(actualContent)).isEqualTo(formatContent(expectedContent));
  }

  @DisplayName("should throw a MarcContentException carrying the cause, instead of silently falling back, "
    + "when the system order content is not valid JSON")
  @Test
  void shouldThrowMarcContentException_whenSystemOrderContentIsMalformed() {
    // arrange: the un-reordered fallback behaviour now lives at the caller (AdditionalFieldsUtil.reorderMarcRecordFields) -
    // reorderFields itself must throw cleanly rather than swallow the failure.
    var sourceOrderContent = "{\"fields\":[{\"245\":{\"subfields\":[{\"a\":\"Title\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var malformedSystemOrderContent = "{not valid json";

    // act / assert
    assertThatThrownBy(() -> MarcJsonFieldOrderer.reorderFields(sourceOrderContent, malformedSystemOrderContent))
      .isInstanceOf(MarcContentException.class)
      .hasCauseInstanceOf(Exception.class);
  }

  private static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }

  private static String formatContent(String content) {
    return content.replaceAll("\\s", "");
  }
}
