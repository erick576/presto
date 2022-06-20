package com.facebook.presto.catalogserver;

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class RemoteSplitManager implements Split
{
    private static final String EMPTY_STRING = "";

    private final Split delegate;
    private final ObjectMapper objectMapper;
    private final DriftClient<CatalogServerClient> catalogServerClient;

    @Inject
    public RemoteSplitManager(
            SplitManager splitManager,
            ObjectMapper objectMapper,
            DriftClient<CatalogServerClient> catalogServerClient)
    {
        this.delegate = requireNonNull(splitManager, "splitManager is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.catalogServerClient = requireNonNull(catalogServerClient, "catalogServerClient is null");
    }

    @Override
    public void addConnectorSplitManager(ConnectorId connectorId, ConnectorSplitManager connectorSplitManager)
    {
        delegate.addConnectorSplitManager(connectorId, connectorSplitManager);
    }

    @Override
    public void removeConnectorSplitManager(ConnectorId connectorId)
    {
        delegate.removeConnectorSplitManager(connectorId);
    }

    @Override
    public SplitSource getSplits(Session session, TableHandle table, ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy, WarningCollector warningCollector)
    {
        String splitsJson = "";
        try {
            splitsJson = catalogServerClient.get().getSplits(
                    session.toSessionRepresentation(),
                    objectMapper.writeValueAsString(table),
                    splitSchedulingStrategy,
                    objectMapper.writeValueAsString(warningCollector));
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        if (!splitsJson.equals(EMPTY_STRING)) {
            return (SplitSource) readValue(splitsJson, new TypeReference<List<SplitSource>>() {});
        }
        return null;
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
}
