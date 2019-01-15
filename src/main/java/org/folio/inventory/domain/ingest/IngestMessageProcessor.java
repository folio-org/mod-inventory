package org.folio.inventory.domain.ingest;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.CollectAll;
import org.folio.inventory.common.MessagingContext;
import org.folio.inventory.domain.*;
import org.folio.inventory.domain.instances.Contributor;
import org.folio.inventory.domain.instances.Identifier;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.resources.ingest.IngestJob;
import org.folio.inventory.resources.ingest.IngestJobState;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.JsonArrayHelper;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class IngestMessageProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TITLE_PROPERTY = "title";
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
    final CollectAll<Holding> allHoldings = new CollectAll<>();

    final MessagingContext context = new MessagingContext(message.headers());
    final JsonObject body = message.body();

    IngestMessages.completed(context.getJobId(), context).send(eventBus);

    final List<JsonObject> records = JsonArrayHelper.toList(body.getJsonArray("records"));

    final JsonObject materialTypes = body.getJsonObject("materialTypes");
    final JsonObject loanTypes = body.getJsonObject("loanTypes");
    final JsonObject locations = body.getJsonObject("locations");
    final JsonObject instanceTypes = body.getJsonObject("instanceTypes");
    final JsonObject identifierTypes = body.getJsonObject("identifierTypes");
    final JsonObject contributorNameTypes = body.getJsonObject("contributorNameTypes");

    final InstanceCollection instanceCollection = storage.getInstanceCollection(context);
    final ItemCollection itemCollection = storage.getItemCollection(context);
    final HoldingCollection holdingCollection = storage.getHoldingCollection(context);

    records.stream()
      .map(record -> {

        List<JsonObject> identifiersJson = JsonArrayHelper.toList(
          record.getJsonArray("identifiers"));

        List<Identifier> identifiers = identifiersJson.stream()
          .map(identifier -> new Identifier(
            identifierTypes.getString("ISBN"),
            identifier.getString("value")))
          .collect(Collectors.toList());

        List<JsonObject> contributorsJson = JsonArrayHelper.toList(
          record.getJsonArray("contributors"));

        List<Contributor> contributors = contributorsJson.stream()
          .map(contributor -> new Contributor(
            contributorNameTypes.getString("Personal name"),
            contributor.getString("name"), "", ""))
          .collect(Collectors.toList());

        if(contributors.isEmpty()) {
          contributors.add(new Contributor(
            contributorNameTypes.getString("Personal name"),
            "Unknown contributor", "", ""));
        }

        return new Instance(
                UUID.randomUUID().toString(),
                null,
                "Local: MODS",
                record.getString(TITLE_PROPERTY),
                instanceTypes.getString("text"))
                .setIdentifiers(identifiers)
                .setContributors(contributors);
      })
      .forEach(instance -> instanceCollection.add(instance, allInstances.receive(),
        failure -> log.error("Instance processing failed: " + failure.getReason())));

      allInstances.collect(instances -> {
        instances.stream().map(instance ->
          new Holding(UUID.randomUUID().toString(), instance.getId(),
            locations.getString("Main Library")))
          .forEach(holding -> holdingCollection.add(holding, allHoldings.receive(),
            failure -> log.error("Holding processing failed: " + failure.getReason())));

        allHoldings.collect(holdings ->
          records.stream().map(record -> {
            //Will fail if have multiple instances with exactly the same title
            Optional<Instance> possibleInstance = instances.stream()
              .filter(instance ->
                StringUtils.equals(instance.getTitle(), record.getString(TITLE_PROPERTY)))
              .findFirst();

            String instanceId = possibleInstance.isPresent()
              ? possibleInstance.get().getId()
              : null;

            Optional<Holding> possibleHolding = holdings.stream()
              .filter(holding ->
                StringUtils.equals(instanceId, holding.instanceId))
              .findFirst();

            String holdingId = possibleHolding.isPresent()
              ? possibleHolding.get().id
              : null;

            return new Item(null,
              holdingId,
              "Available",
              materialTypes.getString("Book") != null
                ? materialTypes.getString("Book")
                : materialTypes.getString("book"),
              loanTypes.getString("Can Circulate") != null
                ? loanTypes.getString("Can Circulate")
                : loanTypes.getString("Can circulate"),
               null)
                    .setBarcode(record.getString("barcode"));
        })
        .forEach(item -> itemCollection.add(item, allItems.receive(),
          failure -> log.error("Item processing failed: " + failure.getReason()))));
      });


    allItems.collect(items ->
      IngestMessages.completed(context.getJobId(), context).send(eventBus));
  }

  private void markIngestCompleted(Message<JsonObject> message) {
    final MessagingContext context = new MessagingContext(message.headers());

    storage.getIngestJobCollection(context).update(
      new IngestJob(context.getJobId(), IngestJobState.COMPLETED),
      v -> log.info(String.format("Ingest job %s completed", context.getJobId())),
      failure -> log.error(
        String.format("Updating ingest job failed: %s", failure.getReason())));
  }
}
