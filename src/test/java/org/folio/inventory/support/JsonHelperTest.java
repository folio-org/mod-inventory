package org.folio.inventory.support;

import static org.folio.inventory.support.JsonHelper.includeIfPresent;
import static org.folio.inventory.support.JsonHelper.putNotNullValues;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JsonHelperTest {

  @Spy
  private JsonObject representation;

  @Test
  void shouldIncludeValue() {
    includeIfPresent(representation, "key", "value");
    verify(representation, times(1)).put("key", "value");
  }

  @Test
  void shouldNotIncludeIfRepresentationIsNull() {
    assertDoesNotThrow(() -> includeIfPresent(null, "key", "value"));
  }

  @Test
  void shouldNotIncludeIfKeyIsNull() {
    includeIfPresent(representation, null, "value");
    verifyNoInteractions(representation);
  }

  @Test
  void shouldNotIncludeIfValueIsNull() {
    includeIfPresent(representation, "key", null);
    verifyNoInteractions(representation);
  }

  @Test
  void shouldNotIncludeOnlyNullValues() {
    final var notNullString = "notNull";
    final var key = "key";
    final var rootKey = "root";
    final var arrayKey = "array";
    final var jsonArrayKey = "jsonArray";
    final var arrayListKey = "arrayList";
    final var value = new JsonObject();
    final var nestedValue = new JsonObject();
    final var list = new ArrayList<JsonObject>();
    final var arrayList = new ArrayList<JsonArray>();
    final var jsonArray = new JsonArray();

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

    final var objResult = representation.getJsonObject(rootKey).getJsonObject(key);
    final var listResult = representation.getJsonArray(arrayKey);
    final var arrayListResult = representation.getJsonArray(arrayListKey).getJsonArray(0).getJsonObject(0);
    final var jsonArrayResult = representation.getJsonArray(jsonArrayKey).getJsonObject(0);
    assertThat(objResult.size(), is(2));
    assertThat(objResult.getValue(notNullString), is(notNullString));
    assertThat(arrayListResult.size(), is(2));
    assertThat(arrayListResult.getValue(notNullString), is(notNullString));
    assertThat(jsonArrayResult.size(), is(2));
    assertThat(jsonArrayResult.getValue(notNullString), is(notNullString));
    assertThat(listResult.getList().size(), is(1));
  }
}
