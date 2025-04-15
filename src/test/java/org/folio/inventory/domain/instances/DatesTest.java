package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.converters.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.folio.inventory.domain.instances.Dates.convertToDates;
import static org.folio.inventory.domain.instances.Dates.datesToJson;
import static org.folio.inventory.domain.instances.Dates.retrieveDatesFromJson;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnitParamsRunner.class)
public class DatesTest {

  @Parameters({
    "1, 1990, 2002",
    "1, 1990, null",
    "1, null, 2022",
    "null, 1990, 2002",
    "null, 1990, null",
  })
  @Test
  public void shouldCreateDatesFromJson(@Nullable String dateTypeId, @Nullable String date1, @Nullable String date2) {
    var dates = convertToDates(datesJson(dateTypeId, date1, date2));

    assertThat(dates.getDateTypeId(), is(dateTypeId));
    assertThat(dates.getDate1(), is(date1));
    assertThat(dates.getDate2(), is(date2));
  }

  @Test
  public void shouldNotCreateDatesFromJsonWhenJsonIsNull() {
    assertThat(convertToDates(null), nullValue());
  }

  @Test
  public void shouldNotCreateDatesFromJsonWhenAllFieldsAreNull() {
    assertThat(convertToDates(datesJson(null, null, null)), nullValue());
  }

  @Parameters({
    "1, 1990, 2002",
    "1, 1990, null",
    "1, null, 2022",
    "null, 1990, 2002",
    "null, 1990, null",
  })
  @Test
  public void shouldConvertDatesToJson(@Nullable String dateTypeId, @Nullable String date1, @Nullable String date2) {
    var json = datesToJson(new Dates(dateTypeId, date1, date2));

    assertThat(json.getString("dateTypeId"), is(dateTypeId));
    assertThat(json.getString("date1"), is(date1));
    assertThat(json.getString("date2"), is(date2));
  }

  @Test
  public void shouldNotConvertDatesToJsonWhenItIsNull() {
    assertThat(datesToJson(null), nullValue());
  }

  @Test
  public void shouldNotConvertDatesToJsonWhenAllFieldsAreNull() {
    assertThat(datesToJson(new Dates(null, null, null)), nullValue());
  }

  @Test
  public void shouldRetrieveDatesFromInstanceJson() {
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
  public void shouldRetrieveDatesFromInstanceJsonFromJsonForStorageObject() {
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
