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
package io.trino.plugin.example;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ExamplePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PulsarClient pulsarClient;

    @Inject
    public ExamplePageSinkProvider() throws PulsarClientException
    {
        // TODO: Pass configuration
        pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof ExampleOutputTableHandle, "tableHandle is not an instance of CassandraOutputTableHandle");
        ExampleOutputTableHandle handle = (ExampleOutputTableHandle) tableHandle;

        try {
            return new ExamplePageSink(
                    handle.getSchemaName(),
                    handle.getTableName(),
                    handle.getColumnNames(),
                    handle.getColumnTypes(),
                    pulsarClient);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof ExampleInsertTableHandle, "tableHandle is not an instance of ConnectorInsertTableHandle");
        ExampleInsertTableHandle handle = (ExampleInsertTableHandle) tableHandle;

        try {
            return new ExamplePageSink(
                    handle.getSchemaName(),
                    handle.getTableName(),
                    handle.getColumnNames(),
                    handle.getColumnTypes(),
                    pulsarClient);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
}
