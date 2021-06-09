package org.folio.inventory.support;

import static org.folio.inventory.support.JsonHelper.includeIfPresent;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import io.vertx.core.json.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

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
}
