package org.folio.inventory.support;

import io.vertx.core.json.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;

import static org.folio.inventory.support.JsonHelper.includeIfPresent;
import static org.folio.inventory.support.JsonHelper.putNotNullValues;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(MockitoJUnitRunner.class)
public class JsonHelperTest {
  @Spy
  private JsonObject representation;

  @Test
  public void shouldIncludeValue() {
    includeIfPresent(representation, "key", "value");
    verify(representation, times(1)).put("key", "value");
  }

  @Test
  public void shouldNotIncludeIfRepresentationIsNull() {
    try {
      includeIfPresent(null, "key", "value");
    } catch (Exception ex) {
      fail("Exception is not expected");
    }
  }

  @Test
  public void shouldNotIncludeIfKeyIsNull() {
    includeIfPresent(representation, null, "value");
    verifyZeroInteractions(representation);
  }

  @Test
  public void shouldNotIncludeIfValueIsNull() {
    includeIfPresent(representation, "key", null);
    verifyZeroInteractions(representation);
  }

  @Test
  public void shouldNotIncludeNullOeEmptyValues() {
    var nutNullString = "notNull";
    var key = "key";
    var rootKey = "root";
    var arrayKey = "array";
    var value = new JsonObject();
    var nestedValue = new JsonObject();
    var list = new ArrayList<JsonObject>();

    nestedValue.put(nutNullString, nutNullString);
    nestedValue.put("null", null);
    nestedValue.put("empty", "");
    value.put(key, nestedValue);
    list.add(nestedValue);
    list.add(null);
    putNotNullValues(representation, rootKey, value);
    putNotNullValues(representation, arrayKey, list);

    var objResult = representation.getJsonObject(rootKey).getJsonObject(key);
    var listResult = representation.getJsonArray(arrayKey);
    assertThat(objResult.size(), is(1));
    assertThat(objResult.getValue(nutNullString), is(nutNullString));
    assertThat(listResult.size(), is(1));
  }
}
