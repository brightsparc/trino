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
import io.trino.spi.type.Type;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ExamplePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PulsarClient pulsarClient;

    @Inject
    public ExamplePageSinkProvider(ExampleConfig exampleConfig) throws PulsarClientException
    {
        pulsarClient = PulsarClient.builder()
                .serviceUrl(exampleConfig.getPulsar().toString())
                .build();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof ExampleOutputTableHandle, "tableHandle is not an instance of CassandraOutputTableHandle");
        ExampleOutputTableHandle handle = (ExampleOutputTableHandle) tableHandle;

        return getPageSink("public", handle.getSchemaName(), handle.getTableName(), handle.getColumnNames(), handle.getColumnTypes());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof ExampleInsertTableHandle, "tableHandle is not an instance of ConnectorInsertTableHandle");
        ExampleInsertTableHandle handle = (ExampleInsertTableHandle) tableHandle;

        return getPageSink("public", handle.getSchemaName(), handle.getTableName(), handle.getColumnNames(), handle.getColumnTypes());
    }

    private ConnectorPageSink getPageSink(String tenant, String schemaName, String tableName, List<String> columnNames, List<Type> columnTypes)
    {
        try {
            // Create a produce for this tenant, schema and table
            String topic = String.format("persistent://%s/%s/%s", tenant, schemaName, tableName);
            // Create a producer, and writeValueAsBytes() when sending
            // see: https://github.com/apache/pulsar/blob/master/site2/docs/schema-get-started.md
            Producer producer = pulsarClient.newProducer()
                    .topic(topic)
                    .create();
            // Return the new page sink
            return new ExamplePageSink(
                    producer,
                    columnNames,
                    columnTypes);
        }
        catch (PulsarClientException ex) {
            ex.printStackTrace();
            return null;
        }
    }
}
