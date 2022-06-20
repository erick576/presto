package com.facebook.presto.split;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager;

public interface Split
{
    void addConnectorSplitManager(ConnectorId connectorId, ConnectorSplitManager connectorSplitManager);

    void removeConnectorSplitManager(ConnectorId connectorId);

    SplitSource getSplits(Session session, TableHandle table, ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy, WarningCollector warningCollector);
}
