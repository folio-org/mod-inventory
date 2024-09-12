package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.converters.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.folio.inventory.domain.instances.Dates.datesFromJson;
import static org.folio.inventory.domain.instances.Dates.datesToJson;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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
    var dates = datesFromJson(datesJson(dateTypeId, date1, date2));

    assertThat(dates.getDateTypeId(), is(dateTypeId));
    assertThat(dates.getDate1(), is(date1));
    assertThat(dates.getDate2(), is(date2));
  }

  @Test
  public void shouldNotCreateDatesFromJsonWhenJsonIsNull() {
    assertThat(datesFromJson(null), nullValue());
  }

  @Test
  public void shouldNotCreateDatesFromJsonWhenAllFieldsAreNull() {
    assertThat(datesFromJson(datesJson(null, null, null)), nullValue());
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

  private JsonObject datesJson(String dateTypeId, String date1, String date2) {
    return new JsonObject().put("dateTypeId", dateTypeId).put("date1", date1).put("date2", date2);
  }
}
