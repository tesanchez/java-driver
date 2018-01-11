/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeStateManager;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.ReplayingEventFilter;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The session implementation.
 *
 * <p>It maintains a {@link ChannelPool} to each node that the {@link LoadBalancingPolicy} set to a
 * non-ignored distance. It listens for distance events and node state events, in order to adjust
 * the pools accordingly.
 *
 * <p>It executes requests by:
 *
 * <ul>
 *   <li>picking the appropriate processor to convert the request into a protocol message.
 *   <li>getting a query plan from the load balancing policy
 *   <li>trying to send the message on each pool, in the order of the query plan
 * </ul>
 */
public class DefaultSession implements CqlSession {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSession.class);

  public static CompletionStage<CqlSession> init(
      InternalDriverContext context,
      Set<InetSocketAddress> contactPoints,
      CqlIdentifier keyspace,
      Set<NodeStateListener> nodeStateListeners) {
    return new DefaultSession(context, contactPoints, keyspace, nodeStateListeners).init();
  }

  private final InternalDriverContext context;
  private final DriverConfig config;
  private final EventExecutor adminExecutor;
  private final String logPrefix;
  private final SingleThreaded singleThreaded;
  private final MetadataManager metadataManager;
  private final RequestProcessorRegistry processorRegistry;

  // This is read concurrently, but only updated from adminExecutor
  private volatile CqlIdentifier keyspace;

  private final ConcurrentMap<Node, ChannelPool> pools =
      new ConcurrentHashMap<>(
          16,
          0.75f,
          // the map will only be updated from adminExecutor
          1);

  // The raw data to reprepare requests on the fly, if we hit a node that doesn't have them in
  // its cache.
  // This is raw protocol-level data, as opposed to the actual instances returned to the client
  // (e.g. DefaultPreparedStatement) which are handled at the protocol level (e.g.
  // CqlPrepareAsyncProcessor). We keep the two separate to avoid introducing a dependency from the
  // session to a particular processor implementation.
  private ConcurrentMap<ByteBuffer, RepreparePayload> repreparePayloads =
      new MapMaker().weakValues().makeMap();

  private DefaultSession(
      InternalDriverContext context,
      Set<InetSocketAddress> contactPoints,
      CqlIdentifier keyspace,
      Set<NodeStateListener> nodeStateListeners) {
    LOG.debug("Creating new session {}", context.sessionName());
    this.adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
    this.context = context;
    this.config = context.config();
    this.singleThreaded = new SingleThreaded(context, contactPoints, nodeStateListeners);
    this.metadataManager = context.metadataManager();
    this.processorRegistry = context.requestProcessorRegistry();
    this.keyspace = keyspace;
    this.logPrefix = context.sessionName();
  }

  private CompletionStage<CqlSession> init() {
    RunOrSchedule.on(adminExecutor, singleThreaded::init);
    return singleThreaded.initFuture;
  }

  @Override
  public String getName() {
    return context.sessionName();
  }

  @Override
  public Metadata getMetadata() {
    return metadataManager.getMetadata();
  }

  @Override
  public boolean isSchemaMetadataEnabled() {
    return metadataManager.isSchemaEnabled();
  }

  @Override
  public CompletionStage<Metadata> setSchemaMetadataEnabled(Boolean newValue) {
    return metadataManager.setSchemaEnabled(newValue);
  }

  @Override
  public CompletionStage<Metadata> refreshSchemaAsync() {
    return metadataManager.refreshSchema(null, true, true);
  }

  @Override
  public CompletionStage<Boolean> checkSchemaAgreementAsync() {
    return context.topologyMonitor().checkSchemaAgreement();
  }

  @Override
  public DriverContext getContext() {
    return context;
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  /**
   * <b>INTERNAL USE ONLY</b> -- switches the session to a new keyspace.
   *
   * <p>This is called by the driver when a {@code USE} query is successfully executed through the
   * session. Calling it from anywhere else is highly discouraged, as an invalid keyspace would
   * wreak havoc (close all connections and make the session unusable).
   */
  public CompletionStage<Void> setKeyspace(CqlIdentifier newKeyspace) {
    CqlIdentifier oldKeyspace = this.keyspace;
    if (Objects.equals(oldKeyspace, newKeyspace)) {
      return CompletableFuture.completedFuture(null);
    }
    if (config.getDefaultProfile().getBoolean(CoreDriverOption.REQUEST_WARN_IF_SET_KEYSPACE)) {
      LOG.warn(
          "[{}] Detected a keyspace change at runtime ({} => {}). "
              + "This is an anti-pattern that should be avoided in production "
              + "(see '{}' in the configuration).",
          logPrefix,
          (oldKeyspace == null) ? "<none>" : oldKeyspace.asInternal(),
          newKeyspace.asInternal(),
          CoreDriverOption.REQUEST_WARN_IF_SET_KEYSPACE.getPath());
    }
    this.keyspace = newKeyspace;
    CompletableFuture<Void> result = new CompletableFuture<>();
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.setKeyspace(newKeyspace, result));
    return result;
  }

  public Map<Node, ChannelPool> getPools() {
    return pools;
  }

  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      RequestT request, GenericType<ResultT> resultType) {
    return processorRegistry
        .processorFor(request, resultType)
        .newHandler(request, this, context, logPrefix)
        .handle();
  }

  public DriverChannel getChannel(Node node, String logPrefix) {
    ChannelPool pool = pools.get(node);
    if (pool == null) {
      LOG.debug("[{}] No pool to {}, skipping", logPrefix, node);
      return null;
    } else {
      DriverChannel channel = pool.next();
      if (channel == null) {
        LOG.trace("[{}] Pool returned no channel for {}, skipping", logPrefix, node);
        return null;
      } else if (channel.closeFuture().isDone()) {
        LOG.trace("[{}] Pool returned closed connection to {}, skipping", logPrefix, node);
        return null;
      } else {
        return channel;
      }
    }
  }

  public ConcurrentMap<ByteBuffer, RepreparePayload> getRepreparePayloads() {
    return repreparePayloads;
  }

  @Override
  public void register(SchemaChangeListener listener) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.register(listener));
  }

  @Override
  public void unregister(SchemaChangeListener listener) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.unregister(listener));
  }

  @Override
  public void register(NodeStateListener listener) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.register(listener));
  }

  @Override
  public void unregister(NodeStateListener listener) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.unregister(listener));
  }

  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @Override
  public CompletionStage<Void> closeAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::close);
    return singleThreaded.closeFuture;
  }

  @Override
  public CompletionStage<Void> forceCloseAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::forceClose);
    return singleThreaded.closeFuture;
  }

  private class SingleThreaded {

    private final InternalDriverContext context;
    private final Set<InetSocketAddress> initialContactPoints;
    private final NodeStateManager nodeStateManager;
    private final ChannelPoolFactory channelPoolFactory;
    private final CompletableFuture<CqlSession> initFuture = new CompletableFuture<>();
    private boolean initWasCalled;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private boolean forceCloseWasCalled;
    private final Object distanceListenerKey;
    private final ReplayingEventFilter<DistanceEvent> distanceEventFilter =
        new ReplayingEventFilter<>(this::processDistanceEvent);
    private final Object stateListenerKey;
    private final ReplayingEventFilter<NodeStateEvent> stateEventFilter =
        new ReplayingEventFilter<>(this::processStateEvent);
    private final Object topologyListenerKey;
    // The pools that we have opened but have not finished initializing yet
    private final Map<Node, CompletionStage<ChannelPool>> pending = new HashMap<>();
    // If we receive events while a pool is initializing, the last one is stored here
    private final Map<Node, DistanceEvent> pendingDistanceEvents = new WeakHashMap<>();
    private final Map<Node, NodeStateEvent> pendingStateEvents = new WeakHashMap<>();
    private Set<SchemaChangeListener> schemaChangeListeners = new HashSet<>();
    private Set<NodeStateListener> nodeStateListeners;

    private SingleThreaded(
        InternalDriverContext context,
        Set<InetSocketAddress> contactPoints,
        Set<NodeStateListener> nodeStateListeners) {
      this.context = context;
      this.nodeStateManager = new NodeStateManager(context);
      this.initialContactPoints = contactPoints;
      this.nodeStateListeners = nodeStateListeners;
      new SchemaListenerNotifier(schemaChangeListeners, context.eventBus(), adminExecutor);
      context
          .eventBus()
          .register(
              NodeStateEvent.class, RunOrSchedule.on(adminExecutor, this::onNodeStateChanged));
      this.channelPoolFactory = context.channelPoolFactory();
      this.distanceListenerKey =
          context
              .eventBus()
              .register(
                  DistanceEvent.class, RunOrSchedule.on(adminExecutor, this::onDistanceEvent));
      this.stateListenerKey =
          context
              .eventBus()
              .register(NodeStateEvent.class, RunOrSchedule.on(adminExecutor, this::onStateEvent));
      this.topologyListenerKey =
          context
              .eventBus()
              .register(
                  TopologyEvent.class, RunOrSchedule.on(adminExecutor, this::onTopologyEvent));
    }

    private void init() {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;
      LOG.debug("[{}] Starting initialization", logPrefix);

      nodeStateListeners.forEach(l -> l.onRegister(DefaultSession.this));

      MetadataManager metadataManager = context.metadataManager();
      metadataManager
          // Store contact points in the metadata right away, the control connection will need them
          // if it has to initialize (if the set is empty, 127.0.0.1 is used as a default).
          .addContactPoints(initialContactPoints)
          .thenCompose(v -> context.topologyMonitor().init())
          .thenCompose(v -> metadataManager.refreshNodes())
          .thenAccept(this::afterInitialNodeListRefresh)
          .exceptionally(
              error -> {
                initFuture.completeExceptionally(error);
                RunOrSchedule.on(adminExecutor, this::close);
                return null;
              });
    }

    private void afterInitialNodeListRefresh(@SuppressWarnings("unused") Void ignored) {
      try {
        boolean protocolWasForced =
            context.config().getDefaultProfile().isDefined(CoreDriverOption.PROTOCOL_VERSION);
        boolean needSchemaRefresh = true;
        if (!protocolWasForced) {
          ProtocolVersion currentVersion = context.protocolVersion();
          ProtocolVersion bestVersion =
              context
                  .protocolVersionRegistry()
                  .highestCommon(metadataManager.getMetadata().getNodes().values());
          if (!currentVersion.equals(bestVersion)) {
            LOG.info(
                "[{}] Negotiated protocol version {} for the initial contact point, "
                    + "but other nodes only support {}, downgrading",
                logPrefix,
                currentVersion,
                bestVersion);
            context.channelFactory().setProtocolVersion(bestVersion);
            ControlConnection controlConnection = context.controlConnection();
            // Might not have initialized yet if there is a custom TopologyMonitor
            if (controlConnection.isInit()) {
              controlConnection.reconnectNow();
              // Reconnection already triggers a full schema refresh
              needSchemaRefresh = false;
            }
          }
        }
        if (needSchemaRefresh) {
          metadataManager.refreshSchema(null, false, true);
        }
        metadataManager.firstSchemaRefreshFuture().thenAccept(this::afterInitialSchemaRefresh);

      } catch (Throwable throwable) {
        initFuture.completeExceptionally(throwable);
      }
    }

    private void afterInitialSchemaRefresh(@SuppressWarnings("unused") Void ignored) {
      try {
        nodeStateManager.markInitialized();
        context.loadBalancingPolicyWrapper().init();
        context.configLoader().onDriverInit(context);
        LOG.debug("[{}] Initialization complete, ready", logPrefix);
        initPools();
      } catch (Throwable throwable) {
        initFuture.completeExceptionally(throwable);
      }
    }

    private void initPools() {
      assert adminExecutor.inEventLoop();

      LOG.debug("[{}] Initializing connection pools", logPrefix);

      // Make sure we don't miss any event while the pools are initializing
      distanceEventFilter.start();
      stateEventFilter.start();

      Collection<Node> nodes = context.metadataManager().getMetadata().getNodes().values();
      List<CompletionStage<ChannelPool>> poolStages = new ArrayList<>(nodes.size());
      for (Node node : nodes) {
        NodeDistance distance = node.getDistance();
        if (distance == NodeDistance.IGNORED) {
          LOG.debug("[{}] Skipping {} because it is IGNORED", logPrefix, node);
        } else if (node.getState() == NodeState.FORCED_DOWN) {
          LOG.debug("[{}] Skipping {} because it is FORCED_DOWN", logPrefix, node);
        } else {
          LOG.debug("[{}] Creating a pool for {}", logPrefix, node);
          poolStages.add(channelPoolFactory.init(node, keyspace, distance, context, logPrefix));
        }
      }
      CompletableFutures.whenAllDone(poolStages, () -> this.onPoolsInit(poolStages), adminExecutor);
    }

    private void onPoolsInit(List<CompletionStage<ChannelPool>> poolStages) {
      assert adminExecutor.inEventLoop();
      LOG.debug("[{}] All pools have finished initializing", logPrefix);
      // We will only propagate an invalid keyspace error if all pools get it
      boolean allInvalidKeyspaces = poolStages.size() > 0;
      for (CompletionStage<ChannelPool> poolStage : poolStages) {
        // Note: pool init always succeeds
        ChannelPool pool = CompletableFutures.getCompleted(poolStage.toCompletableFuture());
        boolean invalidKeyspace = pool.isInvalidKeyspace();
        if (invalidKeyspace) {
          LOG.debug("[{}] Pool to {} reports an invalid keyspace", logPrefix, pool.getNode());
        }
        allInvalidKeyspaces &= invalidKeyspace;
        pools.put(pool.getNode(), pool);
      }
      if (allInvalidKeyspaces) {
        initFuture.completeExceptionally(
            new InvalidKeyspaceException("Invalid keyspace " + keyspace.asCql(true)));
        forceClose();
      } else {
        LOG.debug("[{}] Initialization complete, ready", logPrefix);
        initFuture.complete(DefaultSession.this);
        distanceEventFilter.markReady();
        stateEventFilter.markReady();
      }
    }

    private void onDistanceEvent(DistanceEvent event) {
      assert adminExecutor.inEventLoop();
      distanceEventFilter.accept(event);
    }

    private void onStateEvent(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      stateEventFilter.accept(event);
    }

    private void processDistanceEvent(DistanceEvent event) {
      assert adminExecutor.inEventLoop();
      // no need to check closeWasCalled, because we stop listening for events one closed
      DefaultNode node = event.node;
      NodeDistance newDistance = event.distance;
      if (pending.containsKey(node)) {
        pendingDistanceEvents.put(node, event);
      } else if (newDistance == NodeDistance.IGNORED) {
        ChannelPool pool = pools.remove(node);
        if (pool != null) {
          LOG.debug("[{}] {} became IGNORED, destroying pool", logPrefix, node);
          pool.closeAsync()
              .exceptionally(
                  error -> {
                    Loggers.warnWithException(LOG, "[{}] Error closing pool", logPrefix, error);
                    return null;
                  });
        }
      } else {
        NodeState state = node.getState();
        if (state == NodeState.FORCED_DOWN) {
          LOG.warn(
              "[{}] {} became {} but it is FORCED_DOWN, ignoring", logPrefix, node, newDistance);
          return;
        }
        ChannelPool pool = pools.get(node);
        if (pool == null) {
          LOG.debug(
              "[{}] {} became {} and no pool found, initializing it", logPrefix, node, newDistance);
          CompletionStage<ChannelPool> poolFuture =
              channelPoolFactory.init(node, keyspace, newDistance, context, logPrefix);
          pending.put(node, poolFuture);
          poolFuture
              .thenAcceptAsync(this::onPoolInitialized, adminExecutor)
              .exceptionally(UncaughtExceptions::log);
        } else {
          LOG.debug("[{}] {} became {}, resizing it", logPrefix, node, newDistance);
          pool.resize(newDistance);
        }
      }
    }

    private void processStateEvent(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      // no need to check closeWasCalled, because we stop listening for events once closed
      DefaultNode node = event.node;
      NodeState oldState = event.oldState;
      NodeState newState = event.newState;
      if (pending.containsKey(node)) {
        pendingStateEvents.put(node, event);
      } else if (newState == NodeState.FORCED_DOWN) {
        ChannelPool pool = pools.remove(node);
        if (pool != null) {
          LOG.debug("[{}] {} was FORCED_DOWN, destroying pool", logPrefix, node);
          pool.closeAsync()
              .exceptionally(
                  error -> {
                    Loggers.warnWithException(LOG, "[{}] Error closing pool", logPrefix, error);
                    return null;
                  });
        }
      } else if (oldState == NodeState.FORCED_DOWN
          && newState == NodeState.UP
          && node.getDistance() != NodeDistance.IGNORED) {
        LOG.debug("[{}] {} was forced back UP, initializing pool", logPrefix, node);
        createOrReconnectPool(node);
      }
    }

    private void onTopologyEvent(TopologyEvent event) {
      assert adminExecutor.inEventLoop();
      if (event.type == TopologyEvent.Type.SUGGEST_UP) {
        Node node = context.metadataManager().getMetadata().getNodes().get(event.address);
        if (node.getDistance() != NodeDistance.IGNORED) {
          LOG.debug(
              "[{}] Received a SUGGEST_UP event for {}, reconnecting pool now", logPrefix, node);
          createOrReconnectPool(node);
        }
      }
    }

    private void createOrReconnectPool(Node node) {
      ChannelPool pool = pools.get(node);
      if (pool == null) {
        CompletionStage<ChannelPool> poolFuture =
            channelPoolFactory.init(node, keyspace, node.getDistance(), context, logPrefix);
        pending.put(node, poolFuture);
        poolFuture
            .thenAcceptAsync(this::onPoolInitialized, adminExecutor)
            .exceptionally(UncaughtExceptions::log);
      } else {
        pool.reconnectNow();
      }
    }

    private void onPoolInitialized(ChannelPool pool) {
      assert adminExecutor.inEventLoop();
      Node node = pool.getNode();
      if (closeWasCalled) {
        LOG.debug(
            "[{}] Session closed while a pool to {} was initializing, closing it", logPrefix, node);
        pool.forceCloseAsync();
      } else {
        LOG.debug("[{}] New pool to {} initialized", logPrefix, node);
        if (Objects.equals(keyspace, pool.getInitialKeyspaceName())) {
          reprepareStatements(pool);
        } else {
          // The keyspace changed while the pool was being initialized, switch it now.
          pool.setKeyspace(keyspace)
              .handleAsync(
                  (result, error) -> {
                    if (error != null) {
                      Loggers.warnWithException(
                          LOG, "Error while switching keyspace to " + keyspace, error);
                    }
                    reprepareStatements(pool);
                    return null;
                  },
                  adminExecutor);
        }
      }
    }

    private void reprepareStatements(ChannelPool pool) {
      assert adminExecutor.inEventLoop();
      if (config.getDefaultProfile().getBoolean(CoreDriverOption.REPREPARE_ENABLED)) {
        new ReprepareOnUp(
                logPrefix + "|" + pool.getNode().getConnectAddress(),
                pool,
                repreparePayloads,
                context,
                () -> RunOrSchedule.on(adminExecutor, () -> onPoolReady(pool)))
            .start();
      } else {
        LOG.debug("[{}] Reprepare on up is disabled, skipping", logPrefix);
        onPoolReady(pool);
      }
    }

    private void onPoolReady(ChannelPool pool) {
      assert adminExecutor.inEventLoop();
      Node node = pool.getNode();
      pending.remove(node);
      pools.put(node, pool);
      DistanceEvent distanceEvent = pendingDistanceEvents.remove(node);
      NodeStateEvent stateEvent = pendingStateEvents.remove(node);
      if (stateEvent != null && stateEvent.newState == NodeState.FORCED_DOWN) {
        LOG.debug(
            "[{}] Received {} while the pool was initializing, processing it now",
            logPrefix,
            stateEvent);
        processStateEvent(stateEvent);
      } else if (distanceEvent != null) {
        LOG.debug(
            "[{}] Received {} while the pool was initializing, processing it now",
            logPrefix,
            distanceEvent);
        processDistanceEvent(distanceEvent);
      }
    }

    private void setKeyspace(CqlIdentifier newKeyspace, CompletableFuture<Void> doneFuture) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        doneFuture.complete(null);
        return;
      }
      LOG.debug("[{}] Switching to keyspace {}", logPrefix, newKeyspace);
      List<CompletionStage<Void>> poolReadyFutures = Lists.newArrayListWithCapacity(pools.size());
      for (ChannelPool pool : pools.values()) {
        poolReadyFutures.add(pool.setKeyspace(newKeyspace));
      }
      CompletableFutures.completeFrom(CompletableFutures.allDone(poolReadyFutures), doneFuture);
    }

    private void register(SchemaChangeListener listener) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      // We want onRegister to be called before any event. We can add the listener before, because
      // schema events are processed on this same thread.
      if (schemaChangeListeners.add(listener)) {
        listener.onRegister(DefaultSession.this);
      }
    }

    private void unregister(SchemaChangeListener listener) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      if (schemaChangeListeners.remove(listener)) {
        listener.onUnregister(DefaultSession.this);
      }
    }

    private void register(NodeStateListener listener) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      if (nodeStateListeners.add(listener)) {
        listener.onRegister(DefaultSession.this);
      }
    }

    private void unregister(NodeStateListener listener) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      if (nodeStateListeners.remove(listener)) {
        listener.onUnregister(DefaultSession.this);
      }
    }

    private void onNodeStateChanged(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      if (event.newState == null) {
        nodeStateListeners.forEach(listener -> listener.onRemove(event.node));
      } else if (event.oldState == null && event.newState == NodeState.UNKNOWN) {
        nodeStateListeners.forEach(listener -> listener.onAdd(event.node));
      } else if (event.newState == NodeState.UP) {
        nodeStateListeners.forEach(listener -> listener.onUp(event.node));
      } else if (event.newState == NodeState.DOWN || event.newState == NodeState.FORCED_DOWN) {
        nodeStateListeners.forEach(listener -> listener.onDown(event.node));
      }
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      LOG.debug("[{}] Starting shutdown", logPrefix);

      // Stop listening for events
      context.eventBus().unregister(distanceListenerKey, DistanceEvent.class);
      context.eventBus().unregister(stateListenerKey, NodeStateEvent.class);
      context.eventBus().unregister(topologyListenerKey, TopologyEvent.class);

      for (SchemaChangeListener listener : schemaChangeListeners) {
        listener.onUnregister(DefaultSession.this);
      }
      schemaChangeListeners.clear();
      for (NodeStateListener listener : nodeStateListeners) {
        listener.onUnregister(DefaultSession.this);
      }
      nodeStateListeners.clear();

      closePolicies();

      List<CompletionStage<Void>> childrenCloseStages = new ArrayList<>();
      for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
        childrenCloseStages.add(closeable.closeAsync());
      }
      CompletableFutures.whenAllDone(
          childrenCloseStages, () -> onChildrenClosed(childrenCloseStages), adminExecutor);
    }

    private void forceClose() {
      assert adminExecutor.inEventLoop();
      if (forceCloseWasCalled) {
        return;
      }
      forceCloseWasCalled = true;
      LOG.debug(
          "[{}] Starting forced shutdown (was {}closed before)",
          logPrefix,
          (closeWasCalled ? "" : "not "));

      if (closeWasCalled) {
        // onChildrenClosed has already been scheduled
        for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
          closeable.forceCloseAsync();
        }
      } else {
        context.eventBus().unregister(distanceListenerKey, DistanceEvent.class);
        context.eventBus().unregister(stateListenerKey, NodeStateEvent.class);
        context.eventBus().unregister(topologyListenerKey, TopologyEvent.class);
        for (SchemaChangeListener listener : schemaChangeListeners) {
          listener.onUnregister(DefaultSession.this);
        }
        schemaChangeListeners.clear();
        for (NodeStateListener listener : nodeStateListeners) {
          listener.onUnregister(DefaultSession.this);
        }
        nodeStateListeners.clear();
        closePolicies();
        List<CompletionStage<Void>> childrenCloseStages = new ArrayList<>();
        for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
          childrenCloseStages.add(closeable.forceCloseAsync());
        }
        CompletableFutures.whenAllDone(
            childrenCloseStages, () -> onChildrenClosed(childrenCloseStages), adminExecutor);
      }
    }

    private void onChildrenClosed(List<CompletionStage<Void>> childrenCloseStages) {
      assert adminExecutor.inEventLoop();
      for (CompletionStage<Void> stage : childrenCloseStages) {
        warnIfFailed(stage);
      }
      context
          .nettyOptions()
          .onClose()
          .addListener(
              f -> {
                if (!f.isSuccess()) {
                  closeFuture.completeExceptionally(f.cause());
                } else {
                  LOG.debug("[{}] Shutdown complete", logPrefix);
                  closeFuture.complete(null);
                }
              });
    }

    private void warnIfFailed(CompletionStage<Void> stage) {
      CompletableFuture<Void> future = stage.toCompletableFuture();
      assert future.isDone();
      if (future.isCompletedExceptionally()) {
        Loggers.warnWithException(
            LOG,
            "[{}] Unexpected error while closing",
            logPrefix,
            CompletableFutures.getFailed(future));
      }
    }

    private void closePolicies() {
      for (AutoCloseable closeable :
          ImmutableList.of(
              context.reconnectionPolicy(),
              context.retryPolicy(),
              context.loadBalancingPolicyWrapper(),
              context.speculativeExecutionPolicy(),
              context.addressTranslator(),
              context.configLoader())) {
        try {
          closeable.close();
        } catch (Throwable t) {
          Loggers.warnWithException(LOG, "[{}] Error while closing {}", logPrefix, closeable, t);
        }
      }
    }

    private List<AsyncAutoCloseable> internalComponentsToClose() {
      return ImmutableList.<AsyncAutoCloseable>builder()
          .addAll(pools.values())
          .add(
              nodeStateManager,
              metadataManager,
              context.topologyMonitor(),
              context.controlConnection())
          .build();
    }
  }
}
