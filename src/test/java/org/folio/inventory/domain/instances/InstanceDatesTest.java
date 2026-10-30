package org.folio.inventory.domain.instances;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InstanceDatesTest {

  @DisplayName("should keep dates when instance is serialized with mapFrom and restored with fromJson")
  @Test
  void shouldKeepDates_whenInstanceIsRoundTrippedThroughMapFrom() {
    // arrange
    var dates = new Dates(UUID.randomUUID().toString(), "1990", "2002");
    var instance = instanceWithDates(dates);

    // act
    var restored = Instance.fromJson(JsonObject.mapFrom(instance));

    // assert
    assertThat(restored.getDates()).isEqualTo(dates);
  }

  @DisplayName("should keep dates when instance is copied")
  @Test
  void shouldKeepDates_whenInstanceIsCopied() {
    // arrange
    var dates = new Dates(UUID.randomUUID().toString(), "1990", "2002");

    // act
    var copy = instanceWithDates(dates).copyInstance();

    // assert
    assertThat(copy.getDates()).isEqualTo(dates);
  }

  @DisplayName("should have no dates when instance has none")
  @Test
  void shouldHaveNoDates_whenInstanceHasNone() {
    // act
    var restored = Instance.fromJson(JsonObject.mapFrom(instanceWithDates(null)));

    // assert
    assertThat(restored.getDates()).isNull();
  }

  private static Instance instanceWithDates(Dates dates) {
    return new Instance(UUID.randomUUID().toString(), 1, "hrid", "FOLIO", "title", UUID.randomUUID().toString())
      .setDates(dates);
  }
}
