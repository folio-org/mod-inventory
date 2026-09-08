package org.folio.inventory.domain.instances;

import static org.folio.inventory.domain.instances.Dates.convertToDates;
import static org.folio.inventory.domain.instances.Dates.datesToJson;
import static org.folio.inventory.domain.instances.Dates.retrieveDatesFromJson;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DatesTest {

  @ParameterizedTest
  @CsvSource(nullValues = "null", value = {
    "1, 1990, 2002",
    "1, 1990, null",
    "1, null, 2022",
    "null, 1990, 2002",
    "null, 1990, null",
  })
  void shouldCreateDatesFromJson(String dateTypeId, String date1, String date2) {
    var dates = convertToDates(datesJson(dateTypeId, date1, date2));

    assertThat(dates.dateTypeId(), is(dateTypeId));
    assertThat(dates.date1(), is(date1));
    assertThat(dates.date2(), is(date2));
  }

  @Test
  void shouldNotCreateDatesFromJsonWhenJsonIsNull() {
    assertThat(convertToDates(null), nullValue());
  }

  @Test
  void shouldNotCreateDatesFromJsonWhenAllFieldsAreNull() {
    assertThat(convertToDates(datesJson(null, null, null)), nullValue());
  }

  @ParameterizedTest
  @CsvSource(nullValues = "null", value = {
    "1, 1990, 2002",
    "1, 1990, null",
    "1, null, 2022",
    "null, 1990, 2002",
    "null, 1990, null",
  })
  void shouldConvertDatesToJson(String dateTypeId, String date1, String date2) {
    var json = datesToJson(new Dates(dateTypeId, date1, date2));

    assertThat(json.getString("dateTypeId"), is(dateTypeId));
    assertThat(json.getString("date1"), is(date1));
    assertThat(json.getString("date2"), is(date2));
  }

  @Test
  void shouldNotConvertDatesToJsonWhenItIsNull() {
    assertThat(datesToJson(null), nullValue());
  }

  @Test
  void shouldNotConvertDatesToJsonWhenAllFieldsAreNull() {
    assertThat(datesToJson(new Dates(null, null, null)), nullValue());
  }

  @Test
  void shouldRetrieveDatesFromInstanceJson() {
    JsonObject instanceAsJson = new JsonObject();
    instanceAsJson.put("id", UUID.randomUUID());
    JsonObject datesObject = new JsonObject();
    datesObject.put("date1", "1998");
    datesObject.put("date2", "2025");
    instanceAsJson.put("dates", datesObject);
    JsonObject retrievedDate = retrieveDatesFromJson(instanceAsJson);
    assertNotNull(retrievedDate);
    assertEquals("1998", datesObject.getString("date1"));
    assertEquals("2025", datesObject.getString("date2"));
  }

  @Test
  void shouldRetrieveDatesFromInstanceJsonFromJsonForStorageObject() {
    JsonObject instanceAsJson = new JsonObject();
    instanceAsJson.put("id", UUID.randomUUID());
    JsonObject datesObject = new JsonObject();
    datesObject.put("date1", "1998");
    datesObject.put("date2", "2025");
    JsonObject jsonForStorage = new JsonObject();
    jsonForStorage.put("idForStorage", UUID.randomUUID());
    jsonForStorage.put("dates", datesObject);
    instanceAsJson.put("jsonForStorage", jsonForStorage);
    JsonObject retrievedDate = retrieveDatesFromJson(instanceAsJson);
    assertNotNull(retrievedDate);
    assertEquals("1998", datesObject.getString("date1"));
    assertEquals("2025", datesObject.getString("date2"));
  }

  private JsonObject datesJson(String dateTypeId, String date1, String date2) {
    return new JsonObject().put("dateTypeId", dateTypeId).put("date1", date1).put("date2", date2);
  }
}
