package org.folio.inventory.eventhandlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.function.Consumer;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.dataimport.handlers.actions.AbstractAuthorityEventHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;

import org.folio.Authority;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.handlers.actions.AuthorityUpdateDelegate;
import org.folio.inventory.dataimport.handlers.quickmarc.UpdateAuthorityQuickMarcEventHandler;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.mockito.junit.MockitoRule;

@RunWith(JUnitParamsRunner.class)
public class UpdateAuthorityQuickMarcEventHandlerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/authority-rules.json";
  private static final String AUTHORITY_PATH = "src/test/resources/handlers/authority.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/authority-record.json";
  private static final String AUTHORITY_ID = "b90cb1bc-601f-45d7-b99e-b11efd281dcd";
  private static final Integer AUTHORITY_VERSION = 1;

  @Mock
  private Storage storage;
  @Mock
  private Context context;
  @Mock
  private AuthorityRecordCollection authorityRecordCollection;

  private UpdateAuthorityQuickMarcEventHandler updateAuthorityQuickMarcEventHandler;
  private AuthorityUpdateDelegate authorityUpdateDelegate;
  private JsonObject mappingRules;
  private JsonObject record;
  private Authority existingAuthority;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setUp() throws IOException {
    existingAuthority = new JsonObject(TestUtil.readFileFromPath(AUTHORITY_PATH)).mapTo(Authority.class);
    authorityUpdateDelegate = Mockito.spy(new AuthorityUpdateDelegate(storage));
    updateAuthorityQuickMarcEventHandler = new UpdateAuthorityQuickMarcEventHandler(authorityUpdateDelegate, context);

    when(storage.getAuthorityRecordCollection(any())).thenReturn(authorityRecordCollection);
    doAnswer(invocationOnMock -> {
      Consumer<Success<Authority>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingAuthority));
      return null;
    }).when(authorityRecordCollection).findById(anyString(), any(), any());

    doAnswer(invocationOnMock -> {
      Authority authority = invocationOnMock.getArgument(0);
      Consumer<Success<Authority>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(authority));
      return null;
    }).when(authorityRecordCollection).update(any(), any(), any());

    when(context.getTenantId()).thenReturn("dummy");
    when(context.getToken()).thenReturn("token");
    when(context.getOkapiLocation()).thenReturn("http://localhost");

    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    record = new JsonObject(TestUtil.readFileFromPath(RECORD_PATH));
  }

  @Test
  @Parameters({"true", "false"})
  public void shouldProcessEvent(String isAuthorityExtendedMode) {
    AbstractAuthorityEventHandler.setAuthorityExtendedMode(Boolean.parseBoolean(isAuthorityExtendedMode));
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_AUTHORITY");
    eventPayload.put("MARC_AUTHORITY", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());
    eventPayload.put("RELATED_RECORD_VERSION", AUTHORITY_VERSION.toString());

    Future<Authority> future = updateAuthorityQuickMarcEventHandler.handle(eventPayload);
    Authority updatedAuthority = future.result();

    Assert.assertNotNull(updatedAuthority);
    Assert.assertEquals(AUTHORITY_ID, updatedAuthority.getId());
    Assert.assertEquals(AUTHORITY_VERSION, updatedAuthority.getVersion());

    Assert.assertNotNull(updatedAuthority.getIdentifiers());
    Assert.assertEquals(4, updatedAuthority.getIdentifiers().size());
    Assert.assertNotNull(updatedAuthority.getNotes());

    ArgumentCaptor<Context> argument = ArgumentCaptor.forClass(Context.class);
    verify(authorityUpdateDelegate).handle(any(), any(), argument.capture());
    Assert.assertEquals("token", argument.getValue().getToken());
    Assert.assertEquals("dummy", argument.getValue().getTenantId());
    Assert.assertEquals("http://localhost", argument.getValue().getOkapiLocation());
    AbstractAuthorityEventHandler.setAuthorityExtendedMode(false);
  }

  @Test
  public void shouldCompleteExceptionally_whenRecordIsEmpty() {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_AUTHORITY");
    eventPayload.put("MARC_AUTHORITY", "");
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<Authority> future = updateAuthorityQuickMarcEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
  }

  @Test
  public void shouldSendError() {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(authorityRecordCollection).update(any(), any(), any());

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_AUTHORITY");
    eventPayload.put("MARC_AUTHORITY", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<Authority> future = updateAuthorityQuickMarcEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
  }

  @Test
  public void shouldFailOn_OL_Exception() {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Error updating authority record", 409));
      return null;
    }).when(authorityRecordCollection).update(any(), any(), any());

    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_TYPE", "MARC_AUTHORITY");
    eventPayload.put("MARC_AUTHORITY", record.encode());
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", new JsonObject().encode());

    Future<Authority> future = updateAuthorityQuickMarcEventHandler.handle(eventPayload);

    Assert.assertTrue(future.failed());
    Assert.assertTrue(future.cause() instanceof OptimisticLockingException);
  }

}
