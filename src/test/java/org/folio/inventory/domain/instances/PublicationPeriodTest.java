package org.folio.inventory.domain.instances;

import static org.folio.inventory.domain.instances.PublicationPeriod.publicationPeriodFromJson;
import static org.folio.inventory.domain.instances.PublicationPeriod.publicationPeriodToJson;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.converters.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class PublicationPeriodTest {

  @Parameters({
    "1, 2",
    "null, 2",
    "1, null",
  })
  @Test
  public void shouldCreatePublicationPeriodFromJson(@Nullable Integer start, @Nullable Integer end) {
    var publicationPeriod = publicationPeriodFromJson(publicationPeriodJson(start, end));

    assertThat(publicationPeriod.getStart(), is(start));
    assertThat(publicationPeriod.getEnd(), is(end));
  }

  @Test
  public void shouldNotCreatePublicationPeriodFromJsonWhenJsonIsNull() {
    assertThat(publicationPeriodFromJson(null), nullValue());
  }

  @Test
  public void shouldNotCreatePublicationPeriodFromJsonWhenStartAndEndAreNull() {
    assertThat(publicationPeriodFromJson(publicationPeriodJson(null, null)), nullValue());
  }

  @Parameters({
    "1, 2",
    "null, 2",
    "1, null",
  })
  @Test
  public void shouldConvertPublicationPeriodToJson(@Nullable Integer start, @Nullable Integer end) {
    var json = publicationPeriodToJson(new PublicationPeriod(start, end));

    assertThat(json.getInteger("start"), is(start));
    assertThat(json.getInteger("end"), is(end));
  }

  @Test
  public void shouldNotConvertPublicationPeriodToJsonWhenItIsNull() {
    assertThat(publicationPeriodToJson(null), nullValue());
  }

  @Test
  public void shouldNotConvertPublicationPeriodToJsonWhenStartAndEndAreNull() {
    assertThat(publicationPeriodToJson(new PublicationPeriod(null, null)), nullValue());
  }

  private JsonObject publicationPeriodJson(Integer start, Integer end) {
    return new JsonObject().put("start", start).put("end", end);
  }
}
