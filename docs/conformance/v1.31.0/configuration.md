# Temporal v1.31.0 configuration surface (complete)

> Part of [the v1.31.0 conformance definition](./README.md). This page captures the **complete
> configuration surface of Temporal server v1.31.0** — *what Temporal exposes*, not what tokeira
> supports. It is the denominator for a later triage of what tokeira **must** support.
>
> tokeira makes a deliberate play of **close-to-zero configuration** — `RuntimeConfig` is `Default`
> and not TOML-configurable, mechanical settings are auto-tuned, and there are no env vars on
> invocation (`AGENTS.md`). To justify that stance honestly we must first see, in full, the config
> explosion it is a response to. That is this document.

## Method & source (ground truth)

- **Dynamic config** — every key in `common/dynamicconfig/constants.go` @ `v1.31.0`. Extracted as the
  setting key strings.
- **Static (YAML) config** — the `Config` struct tree in `common/config/config.go` @ `v1.31.0`.

This is a verbatim enumeration of keys/sections, not a re-description; nothing is invented. Defaults and
per-key semantics live in the cited source — captured here is the **surface** (the count and shape).

## Headline

- **Dynamic config: 564 keys.** By top-level prefix:

  | Prefix | Keys | Domain |
  |--------|-----:|--------|
  | `history` | 255 | History service: queue processors, task scheduler, caches, replication, shard mgmt, workflow/update tuning |
  | `frontend` | 79 | Frontend service: rate limits, batch, search-attribute limits, feature toggles, keep-alive |
  | `matching` | 74 | Matching service: task-queue partitions, forwarding, fairness, poller scaling, worker registry, versioning |
  | `system` | 62 | Cross-cutting: caches, visibility, ringpop, deadlock detector, callbacks, feature flags |
  | `worker` | 48 | System workers: scanners, batcher, scheduler, ES processor, per-namespace workers |
  | `limit` | 37 | Size/count limits: blob/history/memo/mutable-state sizes, pending-entity caps, build-id limits |
  | `metrics` | 3 | Metric tag breakdown toggles |
  | `admin` | 3 | Admin dispatch-rate knobs |
  | `rpc` | 1 | Slow-request logging threshold |
  | `dynamicconfig` | 1 | Subscription poll interval |
  | `activity` | 1 | `activity.dispatch` (standalone-activity enable) |

- **Static config:** ~14 top-level YAML sections (deployment/topology), below.

## Working set — what we must absolutely support

> **Scratch / triage area.** We develop the *minimal* set of config tokeira must absolutely support
> here. Nothing below is decided — it is raw material being triaged down. Seeded with Nexus (pending
> triage) and the temporal-dsql bench config (retained just in case).

### Nexus config (to triage)

Operation behaviour & limits (`component.nexusoperations.*`):

```
component.nexusoperations.request.timeout
component.nexusoperations.limit.request.timeout.min
component.nexusoperations.limit.dispatch.task.timeout.min
component.nexusoperations.limit.operation.concurrency        # default 30 (per workflow)
component.nexusoperations.limit.service.name.length          # default 1000
component.nexusoperations.limit.operation.name.length        # default 1000
component.nexusoperations.limit.operation.token.length       # default 4096
component.nexusoperations.limit.header.size                  # default 8192
component.nexusoperations.limit.scheduleToCloseTimeout       # default 0 (no cap)
component.nexusoperations.retryPolicy.initialInterval        # default 1s
component.nexusoperations.retryPolicy.maxInterval            # default 1h
component.nexusoperations.disallowedHeaders
component.nexusoperations.recordCancelRequestCompletionEvents # default true
component.nexusoperations.metrics.tags
component.nexusoperations.callback.endpoint.template         # default "unset"
component.nexusoperations.useSystemCallbackURL               # default true (tokeira deviates — see below)
```

Callback limits & policy (`frontend.*` / `system.*` / `history.*`):

```
frontend.callbackURLMaxLength
frontend.callbackHeaderMaxLength
system.maxCallbacksPerWorkflow
system.maxCHASMCallbacksPerWorkflow
history.enableCHASMCallbacks
```

Endpoint admin limits (`limit.endpoint*`):

```
limit.endpointNameMaxLength
limit.endpointDescriptionMaxSize
limit.endpointExternalURLMaxLength
limit.endpointListDefaultPageSize
limit.endpointListMaxPageSize
```

Endpoint registry / cache / forwarding (`matching.*` / `system.*` / `frontend.*`):

```
matching.nexusEndpointsRefreshInterval
matching.listNexusEndpointsLongPollTimeout
system.nexusReadThroughCacheSize
system.nexusReadThroughCacheTTL
system.refreshNexusEndpointsLongPollTimeout
system.refreshNexusEndpointsMinWait
frontend.allowDeleteNamespaceIfNexusEndpointTarget
frontend.nexusForwardRequestUseEndpointDispatch   # multi-cluster forwarding — likely out
frontend.nexusRequestHeadersBlacklist             # multi-cluster forwarding — likely out
```

tokeira-owned (Wave 0 `PolicyConfig.nexus_completion`; not a Temporal key):

```
nexus_completion.http_addr            # inbound /nexus/callback listener bind (default 0.0.0.0:7253)
nexus_completion.system_callback_url  # URL workers POST completions to (default http://127.0.0.1:7253)
nexus_completion.retry_policy         # 1s initial / 1h max / 2.0 coeff; unbounded attempts
```

Two notes carried from triage discussion: `system_callback_url` is the one **operationally load-bearing**
item — it must be an address Nexus workers can actually reach (the `127.0.0.1` default only works
co-located). And `useSystemCallbackURL=true` is a **deliberate deviation**: tokeira does not implement the
SDK worker-gRPC completion path, so it always resolves worker callbacks to a real HTTP URL.

### Bench / DSQL config — from `temporal-dsql-deploy-ecs` (retained just in case)

> Important DSQL-specific tuning from the temporal-dsql bench deployment
> (`docker/config/dynamicconfig-bench.yaml`). Likely **not** needed — tokeira is DSQL-specialized and
> auto-tunes its mechanical settings — but captured so the hard-won values are not lost.

```yaml
# System
system.enableActivityEagerExecution: true
system.enableNamespaceNotActiveAutoForwarding: true
system.enableNexus: false
system.forceSearchAttributesCacheRefreshOnRead: true
system.transactionSizeLimit: 4000000

# Persistence QPS (per service)
history.persistenceMaxQPS: 15000
matching.persistenceMaxQPS: 15000
frontend.persistenceMaxQPS: 15000

# History service
history.timerProcessorMaxPollRPS: 200
history.timerProcessorUpdateAckInterval: 5s
history.transferProcessorMaxPollRPS: 400
history.transferTaskBatchSize: 200
history.rps: 10000
history.defaultActivityRetryPolicy: {Initial 1s, Max 100s, Backoff 2.0, MaxAttempts 0}

# History cache (critical for benchmark perf)
history.cacheSizeBasedLimit: true
history.hostLevelCacheMaxSizeBytes: 2147483648   # 2GB/host
history.cacheTTL: 1h
history.cacheNonUserContextLockTimeout: 500ms

# Matching (high WPS)
matching.rps: 10000
matching.numTaskqueueWritePartitions: 8          # (also pinned for benchmark-tq-v2)
matching.numTaskqueueReadPartitions: 8
matching.maxTaskBatchSize: 200
matching.getTasksBatchSize: 1000
matching.longPollExpirationInterval: 60s
matching.forwarderMaxOutstandingPolls: 2
matching.forwarderMaxOutstandingTasks: 1000
matching.forwarderMaxRatePerSecond: 2000
matching.syncMatchWaitDuration: 500ms

# Frontend
frontend.rps: 30000
frontend.namespaceRPS: 30000
frontend.namespaceCount: 4000
frontend.visibilityMaxPageSize: 1000
```

DSQL connection/rate-limit env knobs that accompanied the bench deployment (from temporal-dsql; not in
the dynamic-config YAML): `TEMPORAL_SQL_MAX_CONNS=50`, `TEMPORAL_SQL_MAX_IDLE_CONNS=50` (must equal
MaxConns), `TEMPORAL_SQL_MAX_CONN_LIFETIME=55m`, `TEMPORAL_SQL_CONNECTION_TIMEOUT`, plus the
`DSQL_RESERVOIR_*`, `DSQL_TOKEN_BUCKET_*`, `DSQL_SLOT_BLOCK_*`, and `DSQL_*_RATE_LIMITER_*` families.

## Part 1 — Static (YAML) config sections

The server YAML (`Config`) is deployment/topology configuration. Top-level sections:

| Section | Purpose |
|---------|---------|
| `global` | Membership, PProf, TLS (internode/frontend/systemWorker/remoteClusters), Metrics, Authorization |
| `persistence` | Datastore definitions, default/visibility stores, connection pools |
| `log` | Log level/format/output |
| `clusterMetadata` | Cluster name, failover version, initial cluster topology, multi-cluster registry |
| `dcRedirectionPolicy` | Cross-cluster request redirection policy |
| `services` | Per-role (`frontend`/`history`/`matching`/`worker`) RPC: host, grpcPort, membershipPort, bindOnIP, httpPort, keep-alive, client-connection |
| `archival` | History + visibility archival providers and state |
| `publicClient` | Internal client connection to the frontend |
| `dynamicConfigClient` | File-based dynamic-config loader (poll interval, paths) |
| `namespaceDefaults` | Default archival config for new namespaces |
| `otel` (`ExporterConfig`) | OpenTelemetry exporter wiring |
| `visibility` | Visibility store selection + secondary/dual-write |
| `rpc` | (sub-config under services) gRPC/membership/HTTP ports + TLS |

Most of this surface is multi-service topology, persistence wiring, TLS, and multi-cluster — areas
tokeira collapses by construction (single engine, DSQL-specialized storage, no separate role processes).

## Part 2 — Dynamic config (the 564-key explosion)

Listed by prefix, complete. Defaults/semantics are in `common/dynamicconfig/constants.go` @ `v1.31.0`.

### `frontend` (79)

```
frontend.ListWorkersEnabled
frontend.MaxConcurrentAdminBatchOperationPerNamespace
frontend.MaxConcurrentBatchOperationPerNamespace
frontend.MaxExecutionCountBatchOperationPerNamespace
frontend.WorkerCommandsEnabled
frontend.WorkerHeartbeatsEnabled
frontend.WorkflowPauseEnabled
frontend.allowDeleteNamespaceIfNexusEndpointTarget
frontend.allowedExperiments
frontend.callbackHeaderMaxLength
frontend.callbackURLMaxLength
frontend.deleteNamespaceConcurrentDeleteExecutionsActivities
frontend.deleteNamespaceDeleteActivityRPS
frontend.deleteNamespaceDeletePageSize
frontend.deleteNamespaceNamespaceDeleteDelay
frontend.deleteNamespacePagesPerExecution
frontend.disableListVisibilityByFilter
frontend.enableBatcher
frontend.enableCancelWorkerPollsOnShutdown
frontend.enablePrincipalPropagation
frontend.enableSchedules
frontend.enableServerVersionCheck
frontend.enableTokenNamespaceEnforcement
frontend.enableUpdateWorkflowExecution
frontend.enableUpdateWorkflowExecutionAsyncAccepted
frontend.exposeAuthorizerErrors
frontend.globalNamespaceCount
frontend.globalNamespaceRPS
frontend.globalNamespaceRPS.namespaceReplicationInducingAPIs
frontend.globalNamespaceRPS.visibility
frontend.globalNamespaceWorkerDeploymentReadRPS
frontend.globalRPS
frontend.historyHostErrorPercentage
frontend.historyHostSelfErrorProportion
frontend.historyMaxPageSize
frontend.httpAllowedHosts
frontend.keepAliveMaxConnectionAge
frontend.keepAliveMaxConnectionAgeGrace
frontend.keepAliveMaxConnectionIdle
frontend.keepAliveMinTime
frontend.keepAlivePermitWithoutStream
frontend.keepAliveTime
frontend.keepAliveTimeout
frontend.linkMaxSize
frontend.maskInternalErrorDetails
frontend.maxBadBinaries
frontend.maxWorkflowRulesPerNamespace
frontend.maxlinksPerRequest
frontend.namespaceBurstRatio
frontend.namespaceBurstRatio.namespaceReplicationInducingAPIs
frontend.namespaceBurstRatio.visibility
frontend.namespaceCount
frontend.namespaceRPS
frontend.namespaceRPS.namespaceReplicationInducingAPIs
frontend.namespaceRPS.visibility
frontend.nexusForwardRequestUseEndpointDispatch
frontend.nexusRequestHeadersBlacklist
frontend.persistenceDynamicRateLimitingParams
frontend.persistenceGlobalMaxQPS
frontend.persistenceGlobalNamespaceMaxQPS
frontend.persistenceMaxQPS
frontend.persistenceNamespaceMaxQPS
frontend.pollWaitForNamespaceRateLimitToken
frontend.reachabilityQuerySetDurationSinceDefault
frontend.rps
frontend.rps.namespaceReplicationInducingAPIs
frontend.searchAttributesNumberOfKeysLimit
frontend.searchAttributesSizeOfValueLimit
frontend.searchAttributesTotalSizeLimit
frontend.sendRawWorkflowHistory
frontend.shutdownDrainDuration
frontend.shutdownFailHealthCheckDuration
frontend.throttledLogRPS
frontend.visibilityArchivalQueryMaxPageSize
frontend.visibilityMaxPageSize
frontend.workerVersioningDataAPIs
frontend.workerVersioningRuleAPIs
frontend.workerVersioningWorkflowAPIs
frontend.workflowRulesAPIsEnabled
```

### `matching` (74)

```
matching.PollerHistoryTTL
matching.TaskQueueInfoByBuildIdTTL
matching.alignMembershipChange
matching.autoEnableV2
matching.backlogMetricsEmitInterval
matching.backlogNegligibleAge
matching.backlogTaskForwardTimeout
matching.deploymentWorkflowVersion
matching.emitTaskDispatchLatencyAtPoll
matching.enableFairness
matching.enableMigration
matching.enablePollerAutoscalingMetrics
matching.enableWorkerPluginMetrics
matching.ephemeralDataUpdateInterval
matching.fairnessCounter
matching.fairnessKeyRateLimitCacheSize
matching.forwarderMaxChildrenPerNode
matching.forwarderMaxOutstandingPolls
matching.forwarderMaxOutstandingTasks
matching.forwarderMaxRatePerSecond
matching.getTasksBatchSize
matching.getTasksReloadAt
matching.getUserDataLongPollTimeout
matching.getUserDataRefresh
matching.historyMaxPageSize
matching.listNexusEndpointsLongPollTimeout
matching.longPollExpirationInterval
matching.maxDeployments
matching.maxFairnessKeyWeightOverrides
matching.maxTaskBatchSize
matching.maxTaskDeleteBatchSize
matching.maxTaskQueueIdleTime
matching.maxTaskQueuesInDeployment
matching.maxTaskQueuesInDeploymentVersion
matching.maxVersionsInDeployment
matching.maxVersionsInTaskQueue
matching.maxWaitForPollerBeforeFwd
matching.membershipUnloadDelay
matching.minTaskThrottlingBurstSize
matching.nexusEndpointsRefreshInterval
matching.numTaskqueueReadPartitions
matching.numTaskqueueWritePartitions
matching.outstandingTaskAppendsThreshold
matching.persistenceDynamicRateLimitingParams
matching.persistenceGlobalMaxQPS
matching.persistenceGlobalNamespaceMaxQPS
matching.persistenceMaxQPS
matching.persistenceNamespaceMaxQPS
matching.pollerScalingDecisionsPerSecond
matching.pollerScalingMinimumBacklog
matching.pollerScalingWaitTime
matching.priorityBacklogForwarding
matching.priorityLevels
matching.queryPollerUnavailableWindow
matching.queryWorkflowTaskTimeoutLogRate
matching.rps
matching.shutdownDrainDuration
matching.spreadRoutingBatchSize
matching.syncMatchWaitDuration
matching.taskDeleteInterval
matching.throttledLogRPS
matching.updateAckInterval
matching.useNewMatcher
matching.workerRegistryEntryTTL
matching.workerRegistryEvictionInterval
matching.workerRegistryMaxEntries
matching.workerRegistryMinEvictAge
matching.workerRegistryNumBuckets
matching.wv.DeletedRuleRetentionTime
matching.wv.ReachabilityBuildIdVisibilityGracePeriod
matching.wv.VersionDrainageStatusRefreshInterval
matching.wv.VersionDrainageStatusVisibilityGracePeriod
matching.wv.reachabilityCacheClosedWFsTTL
matching.wv.reachabilityCacheOpenWFsTTL
```

### `system` (62)

```
system.clusterMetadataRefreshInterval
system.deadlock.AbortProcess
system.deadlock.DumpGoroutines
system.deadlock.FailHealthCheck
system.deadlock.Interval
system.deadlock.MaxWorkersPerRoot
system.disallowQuery
system.enableActivityEagerExecution
system.enableActivityRetryStampIncrement
system.enableCrossNamespaceCommands
system.enableDataLossMetrics
system.enableDeploymentVersions
system.enableDeployments
system.enableEagerWorkflowStart
system.enableInternodeClientKeepAlive
system.enableInternodeServerKeepAlive
system.enableNamespaceHandoverWait
system.enableNamespaceNotActiveAutoForwarding
system.enableParentClosePolicyWorker
system.enableReadFromHistoryArchival
system.enableReadFromSecondaryVisibility
system.enableReadFromVisibilityArchival
system.enableRingpopTLS
system.enableSendTargetVersionChanged
system.enableStickyQuery
system.enableSuggestCaNOnNewTargetVersion
system.forceNamespaceSelectedAPIAutoForwarding
system.forceSearchAttributesCacheRefreshOnRead
system.historyArchivalState
system.historyHealthSignalMetricsEnabled
system.logAllReqErrors
system.maxCHASMCallbacksPerWorkflow
system.maxCallbacksPerWorkflow
system.namespaceCacheRefreshInterval
system.namespaceMinRetentionGlobal
system.namespaceMinRetentionLocal
system.nexusReadThroughCacheSize
system.nexusReadThroughCacheTTL
system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute
system.operatorRPSRatio
system.persistenceHealthSignalAggregationEnabled
system.persistenceHealthSignalBufferSize
system.persistenceHealthSignalMetricsEnabled
system.persistenceHealthSignalWindowSize
system.persistenceQPSBurstRatio
system.refreshNexusEndpointsLongPollTimeout
system.refreshNexusEndpointsMinWait
system.ringpopApproximateMaxPropagationTime
system.ringpopReplicaPoints
system.secondaryVisibilityWritingMode
system.suppressErrorSetSystemSearchAttribute
system.transactionSizeLimit
system.useRevisionNumberForWorkerVersioning
system.visibilityAllowList
system.visibilityArchivalState
system.visibilityDisableOrderByClause
system.visibilityEnableManualPagination
system.visibilityEnableShadowReadMode
system.visibilityEnableUnifiedQueryConverter
system.visibilityPersistenceMaxReadQPS
system.visibilityPersistenceMaxWriteQPS
system.visibilityPersistenceSlowQueryThreshold
```

### `worker` (48)

```
worker.ESProcessorAckTimeout
worker.ESProcessorBulkActions
worker.ESProcessorBulkSize
worker.ESProcessorFlushInterval
worker.ESProcessorNumOfWorkers
worker.ParentCloseMaxConcurrentActivityExecutionSize
worker.ParentCloseMaxConcurrentActivityTaskPollers
worker.ParentCloseMaxConcurrentWorkflowTaskExecutionSize
worker.ParentCloseMaxConcurrentWorkflowTaskPollers
worker.ScannerMaxConcurrentActivityExecutionSize
worker.ScannerMaxConcurrentActivityTaskPollers
worker.ScannerMaxConcurrentWorkflowTaskExecutionSize
worker.ScannerMaxConcurrentWorkflowTaskPollers
worker.batcherConcurrency
worker.batcherRPS
worker.buildIdScavengerEnabled
worker.buildIdScavengerVisibilityRPS
worker.deleteNamespaceActivityLimitsConfig
worker.enableHistoryRateLimiter
worker.enableNamespaceBatcher
worker.enableScheduler
worker.executionDataDurationBuffer
worker.executionEnableHistoryEventIdValidator
worker.executionScannerPerHostQPS
worker.executionScannerPerShardQPS
worker.executionScannerWorkerCount
worker.executionsScannerEnabled
worker.generateMigrationTaskViaFrontend
worker.historyScannerDataMinAge
worker.historyScannerEnabled
worker.historyScannerVerifyRetention
worker.indexerConcurrency
worker.perNamespaceWorkerCount
worker.perNamespaceWorkerOptions
worker.perNamespaceWorkerStartRate
worker.persistenceDynamicRateLimitingParams
worker.persistenceGlobalMaxQPS
worker.persistenceGlobalNamespaceMaxQPS
worker.persistenceMaxQPS
worker.persistenceNamespaceMaxQPS
worker.protectedNamespaces
worker.removableBuildIdDurationSinceDefault
worker.scannerPersistenceMaxQPS
worker.schedulerLocalActivitySleepLimit
worker.schedulerNamespaceStartWorkflowRPS
worker.stickyCacheSize
worker.taskQueueScannerEnabled
worker.throttledLogRPS
```

### `limit` (37)

```
limit.blobSize.error
limit.blobSize.warn
limit.endpointDescriptionMaxSize
limit.endpointExternalURLMaxLength
limit.endpointListDefaultPageSize
limit.endpointListMaxPageSize
limit.endpointNameMaxLength
limit.historyCount.error
limit.historyCount.suggestContinueAsNew
limit.historyCount.warn
limit.historyMaxPageSize
limit.historySize.error
limit.historySize.suggestContinueAsNew
limit.historySize.warn
limit.maxIDLength
limit.memoSize.error
limit.memoSize.warn
limit.mutableStateActivityFailureSize.error
limit.mutableStateActivityFailureSize.warn
limit.mutableStateSize.error
limit.mutableStateSize.warn
limit.mutableStateTombstoneCountLimit
limit.numPendingActivities.error
limit.numPendingCancelRequests.error
limit.numPendingChildExecutions.error
limit.numPendingSignals.error
limit.reachabilityQueryBuildIds
limit.reachabilityTaskQueueScan
limit.taskQueuesPerBuildId
limit.userMetadataDetailsSize
limit.userMetadataSummarySize
limit.versionBuildIdLimitPerQueue
limit.versionCompatibleSetLimitPerQueue
limit.workerBuildIdSize
limit.wv.AssignmentRuleLimitPerQueue
limit.wv.RedirectRuleLimitPerQueue
limit.wv.RedirectRuleMaxUpstreamBuildIDsPerQueue
```

### `metrics` (3) · `admin` (3) · `rpc` (1) · `dynamicconfig` (1) · `activity` (1)

```
metrics.breakdownByBuildID
metrics.breakdownByPartition
metrics.breakdownByTaskQueue
admin.enableListHistoryTasks
admin.matchingNamespaceTaskqueueToPartitionDispatchRate
admin.matchingNamespaceToPartitionDispatchRate
rpc.slowRequestLoggingThreshold
dynamicconfig.subscriptionPollInterval
activity.dispatch
```

### `history` (255)

The largest surface, dominated by per-queue-processor tuning (transfer / timer / visibility / outbound /
archival processors), task-scheduler and persistence rate limiting, caches, shard management, **replication
(multi-cluster)**, **DLQ**, workflow/update tuning, and CHASM. By tokeira's scope, the replication
(`Replication*`, `replicator*`, `standby*`, `xdc*`) and DLQ (`TaskDLQ*`) keys are multi-cluster / internal
(`excluded.md`); most of the rest is mechanical tuning tokeira auto-tunes rather than exposes.

```
history.ChasmStandbyTaskDiscardDelay
history.EnableHistoryReplicationRateLimiter
history.EnableReplicationReceiverSlowSubmissionFlowControl
history.EnableReplicationTaskBatching
history.EnableReplicationTaskTieredProcessing
history.MaxBufferedQueryCount
history.ReplicationEnableDLQMetrics
history.ReplicationEnableRateLimit
history.ReplicationEnableRateLimitShadowMode
history.ReplicationEnableUpdateWithNewTaskMerge
history.ReplicationExecutableTaskErrorRetryBackoffCoefficient
history.ReplicationExecutableTaskErrorRetryExpiration
history.ReplicationExecutableTaskErrorRetryMaxAttempts
history.ReplicationExecutableTaskErrorRetryMaxInterval
history.ReplicationExecutableTaskErrorRetryWait
history.ReplicationLowPriorityProcessorSchedulerWorkerCount
history.ReplicationLowPriorityTaskParallelism
history.ReplicationMultipleBatches
history.ReplicationProcessorSchedulerQueueSize
history.ReplicationProcessorSchedulerWorkerCount
history.ReplicationProgressCacheMaxSize
history.ReplicationProgressCacheTTL
history.ReplicationReceiverLivenessMultiplier
history.ReplicationReceiverMaxOutstandingTaskCount
history.ReplicationReceiverSlowSubmissionWindow
history.ReplicationReceiverSubmissionLatencyThreshold
history.ReplicationResendMaxBatchCount
history.ReplicationStreamEventLoopRetryMaxAttempts
history.ReplicationStreamSendEmptyTaskDuration
history.ReplicationStreamSenderErrorRetryBackoffCoefficient
history.ReplicationStreamSenderErrorRetryExpiration
history.ReplicationStreamSenderErrorRetryMaxAttempts
history.ReplicationStreamSenderErrorRetryMaxInterval
history.ReplicationStreamSenderErrorRetryWait
history.ReplicationStreamSenderHighPriorityQPS
history.ReplicationStreamSenderLivenessMultiplier
history.ReplicationStreamSenderLowPriorityQPS
history.ReplicationStreamSyncStatusDuration
history.ReplicationTaskApplyTimeout
history.ReplicationTaskFetcherAggregationInterval
history.ReplicationTaskFetcherErrorRetryWait
history.ReplicationTaskFetcherParallelism
history.ReplicationTaskFetcherTimerJitterCoefficient
history.ReplicationTaskProcessorCleanupInterval
history.ReplicationTaskProcessorCleanupJitterCoefficient
history.ReplicationTaskProcessorErrorRetryBackoffCoefficient
history.ReplicationTaskProcessorErrorRetryExpiration
history.ReplicationTaskProcessorErrorRetryMaxAttempts
history.ReplicationTaskProcessorErrorRetryMaxInterval
history.ReplicationTaskProcessorErrorRetryWait
history.ReplicationTaskProcessorHostQPS
history.ReplicationTaskProcessorNoTaskInitialWait
history.ReplicationTaskProcessorShardQPS
history.SkipReapplicationByNamespaceID
history.TaskDLQEnabled
history.TaskDLQErrorPattern
history.TaskDLQInternalErrors
history.TaskDLQUnexpectedErrorAttempts
history.acquireShardConcurrency
history.acquireShardInterval
history.alignMembershipChange
history.allowResetWithPendingChildren
history.archivalBackendMaxRPS
history.archivalProcessorArchiveDelay
history.archivalProcessorMaxPollHostRPS
history.archivalProcessorMaxPollInterval
history.archivalProcessorMaxPollIntervalJitterCoefficient
history.archivalProcessorMaxPollRPS
history.archivalProcessorPollBackoffInterval
history.archivalProcessorSchedulerWorkerCount
history.archivalProcessorUpdateAckInterval
history.archivalProcessorUpdateAckIntervalJitterCoefficient
history.archivalQueueMaxReaderCount
history.archivalTaskBatchSize
history.cacheBackgroundEvict
history.cacheNonUserContextLockTimeout
history.cacheSizeBasedLimit
history.cacheTTL
history.chasmMaxInMemoryPureTasks
history.clientOwnershipCachingEnabled
history.clientOwnershipCachingUnusedTTL
history.defaultActivityRetryPolicy
history.defaultWorkflowRetryPolicy
history.defaultWorkflowTaskTimeout
history.disableFetchRelocatableAttributesFromVisibility
history.discardSpeculativeWorkflowTaskMaximumEventsCount
history.emitShardLagLog
history.enableBestEffortDeleteTasksOnWorkflowUpdate
history.enableCHASMCallbacks
history.enableCHASMSchedulerCreation
history.enableCHASMSchedulerMigration
history.enableCHASMSchedulerRouting
history.enableCHASMSchedulerSentinels
history.enableChasm
history.enableDeleteWorkflowExecutionReplication
history.enableDropRepeatedWorkflowTaskFailures
history.enableHistoryReplicationDLQV2
history.enableHostLevelEventsCache
history.enableParentClosePolicy
history.enableReplicationStream
history.enableSeparateReplicationEnableFlag
history.enableTransitionHistory
history.enableUpdateWithStartRetryOnClosedWorkflowAbort
history.enableUpdateWithStartRetryableErrorOnClosedWorkflowAbort
history.enableUpdateWorkflowModeIgnoreCurrent
history.enableVersionReactivationSignals
history.enableWorkflowExecutionTimeoutTimer
history.enableWorkflowIdReuseStartTimeValidation
history.enableWorkflowTaskStampIncrementOnFailure
history.eventsCacheMaxSizeBytes
history.eventsCacheTTL
history.eventsHostLevelCacheMaxSizeBytes
history.externalPayloadsEnabled
history.healthPersistenceErrorRatio
history.healthPersistenceLatencyFailure
history.healthRPCErrorRatio
history.healthRPCLatencyFailure
history.historyMaxAutoResetPoints
history.hostLevelCacheMaxSize
history.hostLevelCacheMaxSizeBytes
history.longPollExpirationInterval
history.maxInFlightUpdatePayloads
history.maxInFlightUpdates
history.maxLocalParentWorkflowVerificationDuration
history.maxTotalUpdates
history.maxTotalUpdates.suggestContinueAsNewThreshold
history.maximumBufferedEventsBatch
history.maximumBufferedEventsSizeInBytes
history.maximumSignalsPerExecution
history.memoryTimerProcessorSchedulerWorkerCount
history.mutableStateChecksumGenProbability
history.mutableStateChecksumInvalidateBefore
history.mutableStateChecksumVerifyProbability
history.numParentClosePolicySystemWorkflows
history.outboundProcessorMaxPollHostRPS
history.outboundProcessorMaxPollInterval
history.outboundProcessorMaxPollIntervalJitterCoefficient
history.outboundProcessorMaxPollRPS
history.outboundProcessorPollBackoffInterval
history.outboundProcessorUpdateAckInterval
history.outboundProcessorUpdateAckIntervalJitterCoefficient
history.outboundQueue.circuitBreakerSettings
history.outboundQueue.groupLimiter.bufferSize
history.outboundQueue.groupLimiter.concurrency
history.outboundQueue.hostScheduler.maxTaskRPS
history.outboundQueue.standbyTaskMissingEventsDestinationDownErr
history.outboundQueue.standbyTaskMissingEventsDiscardDelay
history.outboundQueueMaxPredicateSize
history.outboundQueueMaxReaderCount
history.outboundQueuePendingTaskCriticalCount
history.outboundQueuePendingTasksMaxCount
history.outboundTaskBatchSize
history.parentClosePolicyThreshold
history.persistenceDynamicRateLimitingParams
history.persistenceGlobalMaxQPS
history.persistenceGlobalNamespaceMaxQPS
history.persistenceMaxQPS
history.persistenceNamespaceMaxQPS
history.persistencePerShardNamespaceMaxQPS
history.queueCriticalSlicesCount
history.queueMaxPredicateSize
history.queueMoveGroupTaskCountBase
history.queueMoveGroupTaskCountMultiplier
history.queuePendingTaskCriticalCount
history.queuePendingTasksMaxCount
history.queueReaderStuckCriticalAttempts
history.replicatorMaxSkipTaskCount
history.replicatorProcessorMaxPollInterval
history.replicatorProcessorMaxPollIntervalJitterCoefficient
history.replicatorTaskBatchSize
history.retentionTimerJitterDuration
history.routingInfoCacheMaxSize
history.routingInfoCacheTTL
history.rps
history.sendRawHistoryBetweenInternalServices
history.sendRawHistoryBytesToMatchingService
history.sendTransientOrSpeculativeWorkflowTaskEvents
history.shardFinalizerTimeout
history.shardFirstUpdateInterval
history.shardIOConcurrency
history.shardIOTimeout
history.shardLingerOwnershipCheckQPS
history.shardLingerTimeLimit
history.shardSyncMinInterval
history.shardUpdateMinInterval
history.shardUpdateMinTasksCompleted
history.shutdownDrainDuration
history.standbyClusterDelay
history.standbyTaskMissingEventsDiscardDelay
history.standbyTaskMissingEventsResendDelay
history.standbyTaskReReplicationContextTimeout
history.startupMembershipJoinDelay
history.taskSchedulerEnableExecutionQueueScheduler
history.taskSchedulerEnableRateLimiter
history.taskSchedulerEnableRateLimiterShadowMode
history.taskSchedulerExecutionQueueSchedulerMaxQueues
history.taskSchedulerExecutionQueueSchedulerQueueConcurrency
history.taskSchedulerExecutionQueueSchedulerQueueTTL
history.taskSchedulerGlobalMaxQPS
history.taskSchedulerGlobalNamespaceMaxQPS
history.taskSchedulerInactiveChannelDeletionDelay
history.taskSchedulerMaxQPS
history.taskSchedulerNamespaceMaxQPS
history.taskSchedulerRateLimiterStartupDelay
history.throttledLogRPS
history.timerProcessorMaxPollHostRPS
history.timerProcessorMaxPollInterval
history.timerProcessorMaxPollIntervalJitterCoefficient
history.timerProcessorMaxPollRPS
history.timerProcessorMaxTimeShift
history.timerProcessorPollBackoffInterval
history.timerProcessorSchedulerActiveRoundRobinWeights
history.timerProcessorSchedulerStandbyRoundRobinWeights
history.timerProcessorSchedulerWorkerCount
history.timerProcessorUpdateAckInterval
history.timerProcessorUpdateAckIntervalJitterCoefficient
history.timerQueueMaxReaderCount
history.timerTaskBatchSize
history.transferProcessorEnsureCloseBeforeDelete
history.transferProcessorMaxPollHostRPS
history.transferProcessorMaxPollInterval
history.transferProcessorMaxPollIntervalJitterCoefficient
history.transferProcessorMaxPollRPS
history.transferProcessorPollBackoffInterval
history.transferProcessorSchedulerActiveRoundRobinWeights
history.transferProcessorSchedulerStandbyRoundRobinWeights
history.transferProcessorSchedulerWorkerCount
history.transferProcessorUpdateAckInterval
history.transferProcessorUpdateAckIntervalJitterCoefficient
history.transferQueueMaxReaderCount
history.transferTaskBatchSize
history.versionMembershipCacheMaxSize
history.versionMembershipCacheTTL
history.versionReactivationSignalCacheMaxSize
history.versionReactivationSignalCacheTTL
history.visibilityProcessorEnableCloseWorkflowCleanup
history.visibilityProcessorEnsureCloseBeforeDelete
history.visibilityProcessorMaxPollHostRPS
history.visibilityProcessorMaxPollInterval
history.visibilityProcessorMaxPollIntervalJitterCoefficient
history.visibilityProcessorMaxPollRPS
history.visibilityProcessorPollBackoffInterval
history.visibilityProcessorRelocateAttributesMinBlobSize
history.visibilityProcessorSchedulerActiveRoundRobinWeights
history.visibilityProcessorSchedulerStandbyRoundRobinWeights
history.visibilityProcessorSchedulerWorkerCount
history.visibilityProcessorUpdateAckInterval
history.visibilityProcessorUpdateAckIntervalJitterCoefficient
history.visibilityQueueMaxReaderCount
history.visibilityTaskBatchSize
history.workflowIdReuseMinimalInterval
history.workflowTaskCriticalAttempt
history.workflowTaskHeartbeatTimeout
history.workflowTaskRetryMaxInterval
history.xdcCacheMaxSizeBytes
```

## Next step (not done here)

This page is **capture only**. The triage — classifying each surface as **must-support** (genuine
v1.31.0 behavioural contract a client/operator can observe), **config-as-constant** (pin the v1.31.0
default as a hardcoded constant; not a knob), or **irrelevant** (deployment topology, multi-cluster,
internal tuning tokeira auto-tunes or collapses) — is the follow-on. The expectation, consistent with the
close-to-zero-config thesis, is that the overwhelming majority is config-as-constant or irrelevant, and
only a small set (e.g. retention bounds, size/count limits that drive admission validation, the Nexus
callback reachability address) is genuine must-support deployment policy.

tokeira's own configuration surface (the "after") and its release readiness are tracked in
[`../../readiness/configuration.md`](../../readiness/configuration.md).
