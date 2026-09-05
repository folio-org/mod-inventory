package org.folio.inventory.dataimport.handlers.matching.preloaders;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.extractMatchProfile;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.processing.matching.loader.query.LoadQueryBuilder;
import org.folio.processing.matching.reader.MatchValueReader;
import org.folio.processing.matching.reader.MatchValueReaderFactory;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;

/**
 * Preloader intended to run some logic to modify existing loading query before passing it to Loader
 * It check whether match expression contains any of defined fields that imply preloading
 * If field is not found - initial query returned
 * If field is found - extract field values, pass them to preloading implementation, then -
 * construct query with new values returned from preloading implementation
 *
 */
public abstract class AbstractPreloader {

  public CompletableFuture<LoadQuery> preload(LoadQuery query, DataImportEventPayload dataImportEventPayload) {
    if (query == null) {
      return CompletableFuture.completedFuture(null);
    }

    MatchProfile matchProfile = extractMatchProfile(dataImportEventPayload);
    MatchDetail matchDetail = matchProfile.getMatchDetails().getFirst();
    MatchExpression matchExpression = matchDetail.getExistingMatchExpression();

    Optional<PreloadingFields> preloadingField = getPreloadingField(matchExpression);
    if (preloadingField.isEmpty()) {
      return CompletableFuture.completedFuture(query);
    }

    List<String> preloadValues = extractPreloadValues(dataImportEventPayload, matchProfile, matchDetail);

    return doPreloading(dataImportEventPayload, preloadingField.get(), preloadValues)
      .thenApply(values -> {
        if (values.isEmpty()) {
          return null;
        }
        matchExpression.setFields(Collections.singletonList(
          new Field().withLabel("field").withValue(getLoaderTargetFieldName())));
        matchDetail.setExistingMatchExpression(matchExpression);

        return LoadQueryBuilder.build(ListValue.of(values.get()), matchDetail);
      });
  }

  protected abstract String getMatchEntityName();

  protected abstract String getLoaderTargetFieldName();

  protected abstract CompletableFuture<Optional<List<String>>> doPreloading(DataImportEventPayload eventPayload,
                                                                            PreloadingFields preloadingField,
                                                                            List<String> loadingParameters);

  /**
   * Reads incoming record match values from event payload
   *
   * @return record field values according to match details
   *
   */
  private List<String> extractPreloadValues(DataImportEventPayload dataImportEventPayload,
                                            MatchProfile matchProfile,
                                            MatchDetail matchDetail) {
    MatchValueReader reader = MatchValueReaderFactory.build(matchProfile.getIncomingRecordType());
    var value = reader.read(dataImportEventPayload, matchDetail);

    if (value.getType().equals(Value.ValueType.STRING)) {
      return Collections.singletonList((String) value.getValue());
    }

    return ((ListValue) value).getValue();
  }

  private Optional<PreloadingFields> getPreloadingField(MatchExpression matchExpression) {
    return Arrays.stream(PreloadingFields.values())
      .filter(preloadingField -> matchExpression.getFields().stream()
        .anyMatch(
          field -> field.getValue().equals(getMatchEntityName() + "." + preloadingField.getExistingMatchField())))
      .findFirst();
  }
}
