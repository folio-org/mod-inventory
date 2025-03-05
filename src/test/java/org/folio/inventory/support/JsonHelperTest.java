package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
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
import static org.mockito.Mockito.verifyNoInteractions;
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
    verifyNoInteractions(representation);
  }

  @Test
  public void shouldNotIncludeIfValueIsNull() {
    includeIfPresent(representation, "key", null);
    verifyNoInteractions(representation);
  }

  @Test
  public void shouldNotIncludeOnlyNullValues() {
    var notNullString = "notNull";
    var key = "key";
    var rootKey = "root";
    var arrayKey = "array";
    var jsonArrayKey = "jsonArray";
    var arrayListKey = "arrayList";
    var value = new JsonObject();
    var nestedValue = new JsonObject();
    var list = new ArrayList<JsonObject>();
    var arrayList = new ArrayList<JsonArray>();
    var jsonArray = new JsonArray();

    nestedValue.put(notNullString, notNullString);
    nestedValue.put("null", null);
    nestedValue.put("empty", "");
    value.put(key, nestedValue);
    list.add(nestedValue);
    list.add(null);
    arrayList.add(JsonArray.of(nestedValue));
    jsonArray.add(nestedValue);
    putNotNullValues(representation, rootKey, value);
    putNotNullValues(representation, arrayKey, list);
    putNotNullValues(representation, arrayListKey, arrayList);
    putNotNullValues(representation, jsonArrayKey, jsonArray);

    var objResult = representation.getJsonObject(rootKey).getJsonObject(key);
    var listResult = representation.getJsonArray(arrayKey);
    var arrayListResult = representation.getJsonArray(arrayListKey).getJsonArray(0).getJsonObject(0);
    var jsonArrayResult = representation.getJsonArray(jsonArrayKey).getJsonObject(0);
    assertThat(objResult.size(), is(2));
    assertThat(objResult.getValue(notNullString), is(notNullString));
    assertThat(arrayListResult.size(), is(2));
    assertThat(arrayListResult.getValue(notNullString), is(notNullString));
    assertThat(jsonArrayResult.size(), is(2));
    assertThat(jsonArrayResult.getValue(notNullString), is(notNullString));
    assertThat(listResult.getList().size(), is(1));
  }
}
