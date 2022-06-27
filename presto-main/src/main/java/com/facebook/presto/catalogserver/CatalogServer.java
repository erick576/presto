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

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.inject.Inject;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThriftService(value = "presto-catalog-server", idlName = "PrestoCatalogServer")
public class CatalogServer
{
    private static final String EMPTY_STRING = "";
    // TODO make cache constants configurable
    private static final Duration CACHE_EXPIRES_AFTER_WRITE_MILLIS = Duration.of(10, MINUTES);
    private static final long CACHE_MAXIMUM_SIZE = 1000;

    private final Metadata metadataProvider;
    private final SessionPropertyManager sessionPropertyManager;
    private final TransactionManager transactionManager;
    private final ObjectMapper objectMapper;

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
    public CatalogServer(MetadataManager metadataProvider, SessionPropertyManager sessionPropertyManager, TransactionManager transactionManager, ObjectMapper objectMapper)
    {
        this.metadataProvider = requireNonNull(metadataProvider, "metadataProvider is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.objectMapper = requireNonNull(objectMapper, "handleResolver is null");

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

    @ThriftMethod
    public MetadataEntry<Boolean> schemaExists(TransactionInfo transactionInfo, SessionRepresentation session, CatalogSchemaName schema)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, schema);
        boolean isCacheHit = isCacheHit(cacheKey, schemaExistsCache);
        Boolean schemaExists = schemaExistsCache.getUnchecked(cacheKey);
        return new MetadataEntry<>(schemaExists, isCacheHit);
    }

    @ThriftMethod
    public MetadataEntry<Boolean> catalogExists(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, catalogName);
        boolean isCacheHit = isCacheHit(cacheKey, catalogExistsCache);
        Boolean catalogExists = catalogExistsCache.getUnchecked(cacheKey);
        return new MetadataEntry<>(catalogExists, isCacheHit);
    }

    @ThriftMethod
    public MetadataEntry<String> listSchemaNames(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, catalogName);
        boolean isCacheHit = isCacheHit(cacheKey, listSchemaNamesCache);
        List<String> schemaNames = listSchemaNamesCache.getUnchecked(cacheKey);
        return schemaNames.isEmpty()
                ? new MetadataEntry<>(EMPTY_STRING, isCacheHit)
                : new MetadataEntry<>(writeValueAsString(schemaNames, objectMapper), isCacheHit);
    }

    @ThriftMethod
    public MetadataEntry<String> getTableHandle(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName table)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, table);
        boolean isCacheHit = isCacheHit(cacheKey, getTableHandleCache);
        Optional<TableHandle> tableHandle = getTableHandleCache.getUnchecked(cacheKey);
        if (!tableHandle.isPresent()) {
            getTableHandleCache.refresh(cacheKey);
            tableHandle = getTableHandleCache.getUnchecked(cacheKey);
        }
        return tableHandle.map(handle -> new MetadataEntry<>(writeValueAsString(handle, objectMapper), isCacheHit))
                .orElseGet(() -> new MetadataEntry<>(EMPTY_STRING, isCacheHit));
    }

    @ThriftMethod
    public MetadataEntry<String> listTables(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, prefix);
        boolean isCacheHit = isCacheHit(cacheKey, listTablesCache);
        List<QualifiedObjectName> tableList = listTablesCache.getUnchecked(cacheKey);
        return tableList.isEmpty()
                ? new MetadataEntry<>(EMPTY_STRING, isCacheHit)
                : new MetadataEntry<>(writeValueAsString(tableList, objectMapper), isCacheHit);
    }

    @ThriftMethod
    public MetadataEntry<String> listViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, prefix);
        boolean isCacheHit = isCacheHit(cacheKey, listViewsCache);
        List<QualifiedObjectName> viewsList = listViewsCache.getUnchecked(cacheKey);
        return viewsList.isEmpty()
                ? new MetadataEntry<>(EMPTY_STRING, isCacheHit)
                : new MetadataEntry<>(writeValueAsString(viewsList, objectMapper), isCacheHit);
    }

    @ThriftMethod
    public MetadataEntry<String> getViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, prefix);
        boolean isCacheHit = isCacheHit(cacheKey, getViewsCache);
        Map<QualifiedObjectName, ViewDefinition> viewsMap = getViewsCache.getUnchecked(cacheKey);
        return viewsMap.isEmpty()
                ? new MetadataEntry<>(EMPTY_STRING, isCacheHit)
                : new MetadataEntry<>(writeValueAsString(viewsMap, objectMapper), isCacheHit);
    }

    @ThriftMethod
    public MetadataEntry<String> getView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, viewName);
        boolean isCacheHit = isCacheHit(cacheKey, getViewCache);
        Optional<ViewDefinition> viewDefinition = getViewCache.getUnchecked(cacheKey);
        return viewDefinition.map(view -> new MetadataEntry<>(writeValueAsString(view, objectMapper), isCacheHit))
                .orElseGet(() -> new MetadataEntry<>(EMPTY_STRING, isCacheHit));
    }

    @ThriftMethod
    public MetadataEntry<String> getMaterializedView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, viewName);
        boolean isCacheHit = isCacheHit(cacheKey, getMaterializedViewCache);
        Optional<ConnectorMaterializedViewDefinition> connectorMaterializedViewDefinition = getMaterializedViewCache.getUnchecked(cacheKey);
        return connectorMaterializedViewDefinition.map(materializedView -> new MetadataEntry<>(writeValueAsString(materializedView, objectMapper), isCacheHit))
                .orElseGet(() -> new MetadataEntry<>(EMPTY_STRING, isCacheHit));
    }

    @ThriftMethod
    public MetadataEntry<String> getReferencedMaterializedViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName tableName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, tableName);
        boolean isCacheHit = isCacheHit(cacheKey, getReferencedMaterializedViewsCache);
        List<QualifiedObjectName> referencedMaterializedViewsList = getReferencedMaterializedViewsCache.getUnchecked(cacheKey);
        return referencedMaterializedViewsList.isEmpty()
                ? new MetadataEntry<>(EMPTY_STRING, isCacheHit)
                : new MetadataEntry<>(writeValueAsString(referencedMaterializedViewsList, objectMapper), isCacheHit);
    }

    /*
        Loading Cache Methods
     */

    private Boolean loadCatalogExists(CacheKey key)
    {
        return metadataProvider.catalogExists(key.getSession().toSession(sessionPropertyManager), (String) key.getKey());
    }

    private Boolean loadSchemaExists(CacheKey key)
    {
        return metadataProvider.schemaExists(key.getSession().toSession(sessionPropertyManager), (CatalogSchemaName) key.getKey());
    }

    private List<String> loadListSchemaNames(CacheKey key)
    {
        return metadataProvider.listSchemaNames(key.getSession().toSession(sessionPropertyManager), (String) key.getKey());
    }

    private Optional<TableHandle> loadGetTableHandle(CacheKey key)
    {
        return metadataProvider.getTableHandle(key.getSession().toSession(sessionPropertyManager), (QualifiedObjectName) key.getKey());
    }

    private List<QualifiedObjectName> loadListTables(CacheKey key)
    {
        return metadataProvider.listTables(key.getSession().toSession(sessionPropertyManager), (QualifiedTablePrefix) key.getKey());
    }

    private List<QualifiedObjectName> loadListViews(CacheKey key)
    {
        return metadataProvider.listViews(key.getSession().toSession(sessionPropertyManager), (QualifiedTablePrefix) key.getKey());
    }

    private Map<QualifiedObjectName, ViewDefinition> loadGetViews(CacheKey key)
    {
        return metadataProvider.getViews(key.getSession().toSession(sessionPropertyManager), (QualifiedTablePrefix) key.getKey());
    }

    private Optional<ViewDefinition> loadGetView(CacheKey key)
    {
        return metadataProvider.getView(key.getSession().toSession(sessionPropertyManager), (QualifiedObjectName) key.getKey());
    }

    private Optional<ConnectorMaterializedViewDefinition> loadGetMaterializedView(CacheKey key)
    {
        return metadataProvider.getMaterializedView(key.getSession().toSession(sessionPropertyManager), (QualifiedObjectName) key.getKey());
    }

    private List<QualifiedObjectName> loadGetReferencedMaterializedViews(CacheKey key)
    {
        return metadataProvider.getReferencedMaterializedViews(key.getSession().toSession(sessionPropertyManager), (QualifiedObjectName) key.getKey());
    }

    private String writeValueAsString(Object value, ObjectMapper objectMapper)
    {
        try {
            return objectMapper.writeValueAsString(value);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isCacheHit(CacheKey cacheKey, LoadingCache loadingCache)
    {
        return loadingCache.getIfPresent(cacheKey) != null;
    }

    private static class CacheKey<T>
    {
        private final SessionRepresentation session;
        private final T key;

        private CacheKey(SessionRepresentation session, T key)
        {
            this.session = session;
            this.key = key;
        }

        public SessionRepresentation getSession()
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
