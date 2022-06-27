/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.catalogserver;

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.CatalogMetadata;
import com.facebook.presto.metadata.DelegatingMetadataManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.RuntimeMetricName.CATALOG_SERVER_CACHE_HIT_COUNT;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

// TODO : Use thrift to serialize metadata objects instead of json serde on catalog server in the future
// TODO : Add e2e tests for this class
public class RemoteMetadataManager
        extends DelegatingMetadataManager
{
    // TODO make cache constants configurable
    private static final Duration CACHE_EXPIRES_AFTER_WRITE_MILLIS = Duration.of(10, MINUTES);
    private static final long CACHE_MAXIMUM_SIZE = 1;

    private final TransactionManager transactionManager;
    private final ObjectMapper objectMapper;
    private final DriftClient<CatalogServerClient> catalogServerClient;

    private final LoadingCache<CacheKey, Boolean> catalogExistsCache;
    private final LoadingCache<CacheKey, Boolean> schemaExistsCache;
    private final LoadingCache<CacheKey, List<String>> listSchemaNamesCache;
    private final LoadingCache<CacheKey, Optional<TableHandle>> getTableHandleCache;
    private final LoadingCache<CacheKey, List<QualifiedObjectName>> listTablesCache;
    private final LoadingCache<CacheKey, List<QualifiedObjectName>> listViewsCache;
    private final LoadingCache<CacheKey, Map<QualifiedObjectName, ViewDefinition>> getViewsCache;
    private final LoadingCache<CacheKey, Optional<ViewDefinition>> getViewCache;
    private final LoadingCache<CacheKey, Optional<ConnectorMaterializedViewDefinition>> getMaterializedViewCache;
    private final LoadingCache<CacheKey, List<QualifiedObjectName>> getReferencedMaterializedViewsCache;

    @Inject
    public RemoteMetadataManager(
            MetadataManager metadataManager,
            TransactionManager transactionManager,
            ObjectMapper objectMapper,
            DriftClient<CatalogServerClient> catalogServerClient)
    {
        super(metadataManager);
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.catalogServerClient = requireNonNull(catalogServerClient, "catalogServerClient is null");

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .maximumSize(CACHE_MAXIMUM_SIZE)
                .expireAfterWrite(CACHE_EXPIRES_AFTER_WRITE_MILLIS.toMillis(), MILLISECONDS);

        this.catalogExistsCache = cacheBuilder.build(CacheLoader.from(this::loadCatalogExists));
        this.schemaExistsCache = cacheBuilder.build(CacheLoader.from(this::loadSchemaExists));
        this.listSchemaNamesCache = cacheBuilder.build(CacheLoader.from(this::loadListSchemaNames));
        this.getTableHandleCache = cacheBuilder.build(CacheLoader.from(this::loadGetTableHandle));
        this.listTablesCache = cacheBuilder.build(CacheLoader.from(this::loadListTables));
        this.listViewsCache = cacheBuilder.build(CacheLoader.from(this::loadListViews));
        this.getViewsCache = cacheBuilder.build(CacheLoader.from(this::loadGetViews));
        this.getViewCache = cacheBuilder.build(CacheLoader.from(this::loadGetView));
        this.getMaterializedViewCache = cacheBuilder.build(CacheLoader.from(this::loadGetMaterializedView));
        this.getReferencedMaterializedViewsCache = cacheBuilder.build(CacheLoader.from(this::loadGetReferencedMaterializedViews));
    }

    /*
        Metadata Manager Methods
     */

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, schema);
        incrementCacheHitCount(session, isCacheHit(cacheKey, schemaExistsCache));
        return schemaExistsCache.getUnchecked(cacheKey);
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, catalogName);
        incrementCacheHitCount(session, isCacheHit(cacheKey, catalogExistsCache));
        return catalogExistsCache.getUnchecked(cacheKey);
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, catalogName);
        incrementCacheHitCount(session, isCacheHit(cacheKey, listSchemaNamesCache));
        return listSchemaNamesCache.getUnchecked(cacheKey);
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, table);
        incrementCacheHitCount(session, isCacheHit(cacheKey, getTableHandleCache));
        Optional<TableHandle> tableHandle = getTableHandleCache.getUnchecked(cacheKey);
        if (!tableHandle.isPresent()) {
            getTableHandleCache.refresh(cacheKey);
            tableHandle = getTableHandleCache.getUnchecked(cacheKey);
        }
        if (tableHandle.isPresent()) {
            Optional<CatalogMetadata> catalogMetadata = this.transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), table.getCatalogName());
            if (catalogMetadata.isPresent()) {
                tableHandle = Optional.of(new TableHandle(
                        tableHandle.get().getConnectorId(),
                        tableHandle.get().getConnectorHandle(),
                        catalogMetadata.get().getTransactionHandleFor(tableHandle.get().getConnectorId()),
                        tableHandle.get().getLayout()));
            }
        }
        return tableHandle;
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, prefix);
        incrementCacheHitCount(session, isCacheHit(cacheKey, listTablesCache));
        return listTablesCache.getUnchecked(cacheKey);
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, prefix);
        incrementCacheHitCount(session, isCacheHit(cacheKey, listViewsCache));
        return listViewsCache.getUnchecked(cacheKey);
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, prefix);
        incrementCacheHitCount(session, isCacheHit(cacheKey, getViewsCache));
        return getViewsCache.getUnchecked(cacheKey);
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, viewName);
        incrementCacheHitCount(session, isCacheHit(cacheKey, getViewCache));
        return getViewCache.getUnchecked(cacheKey);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, viewName);
        incrementCacheHitCount(session, isCacheHit(cacheKey, getMaterializedViewCache));
        return getMaterializedViewCache.getUnchecked(cacheKey);
    }

    @Override
    public List<QualifiedObjectName> getReferencedMaterializedViews(Session session, QualifiedObjectName tableName)
    {
        CacheKey cacheKey = new CacheKey(transactionManager.getTransactionInfo(session.getRequiredTransactionId()), session, tableName);
        incrementCacheHitCount(session, isCacheHit(cacheKey, getReferencedMaterializedViewsCache));
        return getReferencedMaterializedViewsCache.getUnchecked(cacheKey);
    }

    /*
        Loading Cache Methods
     */

    private Boolean loadCatalogExists(CacheKey key)
    {
        MetadataEntry<Boolean> metadataEntry = catalogServerClient.get().catalogExists(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (String) key.getKey());
        incrementCacheHitCount(key.getSession(), metadataEntry.getIsCacheHit());
        return metadataEntry.getValue();
    }

    private Boolean loadSchemaExists(CacheKey key)
    {
        MetadataEntry<Boolean> metadataEntry = catalogServerClient.get().schemaExists(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (CatalogSchemaName) key.getKey());
        incrementCacheHitCount(key.getSession(), metadataEntry.getIsCacheHit());
        return metadataEntry.getValue();
    }

    private List<String> loadListSchemaNames(CacheKey key)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().listSchemaNames(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (String) key.getKey());
        incrementCacheHitCount(key.getSession(), metadataEntry.getIsCacheHit());
        return metadataEntry.getValue().isEmpty()
                ? ImmutableList.of()
                : readValue(metadataEntry.getValue(), new TypeReference<List<String>>() {});
    }

    private Optional<TableHandle> loadGetTableHandle(CacheKey key)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getTableHandle(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (QualifiedObjectName) key.getKey());
        return metadataEntry.getValue().isEmpty()
                ? Optional.empty()
                : Optional.of(readValue(metadataEntry.getValue(), new TypeReference<TableHandle>() {}));
    }

    private List<QualifiedObjectName> loadListTables(CacheKey key)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().listTables(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (QualifiedTablePrefix) key.getKey());
        incrementCacheHitCount(key.getSession(), metadataEntry.getIsCacheHit());
        return metadataEntry.getValue().isEmpty()
                ? ImmutableList.of()
                : readValue(metadataEntry.getValue(), new TypeReference<List<QualifiedObjectName>>() {});
    }

    private List<QualifiedObjectName> loadListViews(CacheKey key)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().listViews(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (QualifiedTablePrefix) key.getKey());
        incrementCacheHitCount(key.getSession(), metadataEntry.getIsCacheHit());
        return metadataEntry.getValue().isEmpty()
                ? ImmutableList.of()
                : readValue(metadataEntry.getValue(), new TypeReference<List<QualifiedObjectName>>() {});
    }

    private Map<QualifiedObjectName, ViewDefinition> loadGetViews(CacheKey key)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getViews(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (QualifiedTablePrefix) key.getKey());
        incrementCacheHitCount(key.getSession(), metadataEntry.getIsCacheHit());
        return metadataEntry.getValue().isEmpty()
                ? ImmutableMap.of()
                : readValue(metadataEntry.getValue(), new TypeReference<Map<QualifiedObjectName, ViewDefinition>>() {});
    }

    private Optional<ViewDefinition> loadGetView(CacheKey key)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getView(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (QualifiedObjectName) key.getKey());
        incrementCacheHitCount(key.getSession(), metadataEntry.getIsCacheHit());
        return metadataEntry.getValue().isEmpty()
                ? Optional.empty()
                : Optional.of(readValue(metadataEntry.getValue(), new TypeReference<ViewDefinition>() {}));
    }

    private Optional<ConnectorMaterializedViewDefinition> loadGetMaterializedView(CacheKey key)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getMaterializedView(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (QualifiedObjectName) key.getKey());
        incrementCacheHitCount(key.getSession(), metadataEntry.getIsCacheHit());
        return metadataEntry.getValue().isEmpty()
                ? Optional.empty()
                : Optional.of(readValue(metadataEntry.getValue(), new TypeReference<ConnectorMaterializedViewDefinition>() {}));
    }

    private List<QualifiedObjectName> loadGetReferencedMaterializedViews(CacheKey key)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getReferencedMaterializedViews(
                transactionManager.getTransactionInfo(key.getSession().getRequiredTransactionId()),
                key.getSession().toSessionRepresentation(),
                (QualifiedObjectName) key.getKey());
        incrementCacheHitCount(key.getSession(), metadataEntry.getIsCacheHit());
        return metadataEntry.getValue().isEmpty()
                ? ImmutableList.of()
                : readValue(metadataEntry.getValue(), new TypeReference<List<QualifiedObjectName>>() {});
    }

    private <T> T readValue(String content, TypeReference<T> valueTypeRef)
    {
        try {
            return objectMapper.readValue(content, valueTypeRef);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void incrementCacheHitCount(Session session, boolean isCacheHit)
    {
        session.getRuntimeStats().addMetricValue(CATALOG_SERVER_CACHE_HIT_COUNT, NONE, isCacheHit ? 1 : 0);
    }

    private boolean isCacheHit(CacheKey cacheKey, LoadingCache loadingCache)
    {
        return loadingCache.getIfPresent(cacheKey) != null;
    }

    private static class CacheKey<T>
    {
        private final TransactionInfo transactionInfo;
        private final Session session;
        private final T key;

        private CacheKey(TransactionInfo transactionInfo, Session session, T key)
        {
            this.transactionInfo = transactionInfo;
            this.session = session;
            this.key = key;
        }

        public TransactionInfo getTransactionInfo()
        {
            return transactionInfo;
        }

        public Session getSession()
        {
            return session;
        }

        public T getKey()
        {
            return key;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(key, cacheKey.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key);
        }
    }
}
