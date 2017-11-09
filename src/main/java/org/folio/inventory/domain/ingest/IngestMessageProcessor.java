package org.folio.inventory.domain.ingest;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.CollectAll;
import org.folio.inventory.common.MessagingContext;
import org.folio.inventory.domain.*;
import org.folio.inventory.resources.ingest.IngestJob;
import org.folio.inventory.resources.ingest.IngestJobState;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.JsonArrayHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class IngestMessageProcessor {
  private final Storage storage;

  public IngestMessageProcessor(final Storage storage) {
    this.storage = storage;
  }

  public void register(EventBus eventBus) {
    eventBus.consumer(Messages.START_INGEST.Address, recordsMessageHandler(eventBus));

    eventBus.consumer(Messages.INGEST_COMPLETED.Address, this::markIngestCompleted);
  }

  private Handler<Message<JsonObject>> recordsMessageHandler(EventBus eventBus) {
    return message -> processRecordsMessage(message, eventBus);
  }

  private void processRecordsMessage(Message<JsonObject> message, final EventBus eventBus) {
    final CollectAll<Item> allItems = new CollectAll<>();
    final CollectAll<Instance> allInstances = new CollectAll<>();

    final MessagingContext context = new MessagingContext(message.headers());
    final JsonObject body = message.body();

    IngestMessages.completed(context.getHeader("jobId"), context).send(eventBus);

    final List<JsonObject> records = JsonArrayHelper.toList(body.getJsonArray("records"));
    final JsonObject materialTypes = body.getJsonObject("materialTypes");
    final JsonObject loanTypes = body.getJsonObject("loanTypes");
    final JsonObject locations = body.getJsonObject("locations");

    final InstanceCollection instanceCollection = storage.getInstanceCollection(context);
    final ItemCollection itemCollection = storage.getItemCollection(context);

    records.stream()
      .map(record -> {
        List<JsonObject> identifiersJson = JsonArrayHelper.toList(
          record.getJsonArray("identifiers"));

        List<Identifier> identifiers = identifiersJson.stream()
          .map(identifier -> new Identifier(identifier.getString("namespace"),
            identifier.getString("value")))
          .collect(Collectors.toList());

        return new Instance(record.getString("title"), identifiers);
      })
      .forEach(instance -> {
        instanceCollection.add(instance, allInstances.receive(),
          failure -> System.out.println("Instance processing failed: " + failure.getReason()));
      });

      allInstances.collect(instances -> {
        records.stream().map(record -> {
          Optional<Instance> possibleInstance = instances.stream()
            .filter(instance -> StringUtils.equals(instance.title, record.getString("title")))
            .findFirst();

          String instanceId = possibleInstance.isPresent()
            ? possibleInstance.get().id
            : null;

          return new Item(null,
            record.getString("title"),
            record.getString("barcode"),
            instanceId,
            "Available",
            materialTypes.getString("Book"),
            locations.getString("Main Library"),
            null,
            loanTypes.getString("Can Circulate"),
            null);
        })
        .forEach(item -> {
          itemCollection.add(item, allItems.receive(),
            failure -> System.out.println("Item processing failed: " + failure.getReason()));
        });
      });

    allItems.collect(items -> {
      IngestMessages.completed(context.getHeader("jobId"), context).send(eventBus);
    });
  }

  private void markIngestCompleted(Message message) {
    final MessagingContext context = new MessagingContext(message.headers());

    storage.getIngestJobCollection(context).update(
      new IngestJob(context.getHeader("jobId"), IngestJobState.COMPLETED),
      v -> { },
      failure -> System.out.println(
        String.format("Updating ingest job failed: %s", failure.getReason())));
  }

  private Map<String, String> referenceRecordMap(JsonArray array) {

    Map<String, String> map = new HashMap<>();

    JsonArrayHelper.toList(array).stream()
      .forEach(record -> map.put(record.getString("name"), record.getString("id")));

    return map;
  }
}
