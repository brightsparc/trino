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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.pulsar.client.api.Producer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ExamplePageSink
        implements ConnectorPageSink
{
    private final Producer<byte[]> producer;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final ObjectMapper objectMapper;

    public ExamplePageSink(
            Producer<byte[]> producer,
            List<String> columnNames,
            List<Type> columnTypes)
    {
        this.producer = requireNonNull(producer, "producer is null");
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        List<CompletableFuture<?>> messages = new ArrayList<>();

        for (int position = 0; position < page.getPositionCount(); position++) {
            // Declare json node output
            ObjectNode node = this.objectMapper.createObjectNode();

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                appendColumn(node, page, position, channel);
            }

            try {
                // Convert to JSON as bytes
                byte[] jsonAsBytes = this.objectMapper.writer().writeValueAsBytes(node);
                // Add to messages
                messages.add(this.producer.sendAsync(jsonAsBytes));
            }
            catch (JsonProcessingException ex) {
                ex.printStackTrace();
            }
        }

        // Return a future for all messages
        return CompletableFuture.allOf(messages.toArray(new CompletableFuture[messages.size()]));
    }

    private void appendColumn(ObjectNode node, Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);
        String name = columnNames.get(channel);
        Type type = columnTypes.get(channel);
        if (block.isNull(position)) {
            node.set(name, node.nullNode());
        }
        else if (BOOLEAN.equals(type)) {
            node.put(name, type.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            node.put(name, type.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            node.put(name, toIntExact(type.getLong(block, position)));
        }
        else if (SMALLINT.equals(type)) {
            node.put(name, Shorts.checkedCast(type.getLong(block, position)));
        }
        else if (TINYINT.equals(type)) {
            node.put(name, SignedBytes.checkedCast(type.getLong(block, position)));
        }
        else if (DOUBLE.equals(type)) {
            node.put(name, type.getDouble(block, position));
        }
        else if (REAL.equals(type)) {
            node.put(name, intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (DATE.equals(type)) {
            // TODO: support date as long?
            node.put(name, type.getLong(block, position));
        }
        else if (TIMESTAMP_TZ_MILLIS.equals(type)) {
            // TODO: store timestamp as long?
            node.put(name, unpackMillisUtc(type.getLong(block, position)));
        }
        else if (type instanceof VarcharType) {
            node.put(name, type.getSlice(block, position).toStringUtf8());
        }
        else if (VARBINARY.equals(type)) {
            node.put(name, type.getSlice(block, position).toByteBuffer().array());
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // Flush and then return empty immutable list
        return this.producer.flushAsync().thenApply(v -> ImmutableList.of());
    }

    @Override
    public void abort() {}
}