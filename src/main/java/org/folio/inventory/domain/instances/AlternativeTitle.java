/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class AlternativeTitle {
  public static final String ALTERNATIVE_TITLE_TYPE_ID_KEY = "alternativeTitleTypeId";
  public static final String ALTERNATIVE_TITLE_KEY = "alternativeTitle";

  public final String alternativeTitleTypeId;
  public final String alternativeTitle;

  public AlternativeTitle(String alternativeTitleTypeId, String alternativeTitle) {
    this.alternativeTitleTypeId = alternativeTitleTypeId;
    this.alternativeTitle = alternativeTitle;
  }

  public AlternativeTitle (JsonObject json) {
    this(json.getString(ALTERNATIVE_TITLE_TYPE_ID_KEY), json.getString(ALTERNATIVE_TITLE_KEY));
  }
}
