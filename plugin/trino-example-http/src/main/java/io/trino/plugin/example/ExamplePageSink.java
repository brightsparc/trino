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

//import com.fasterxml.jackson.databind.node.ObjectNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.sql.Timestamp;
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
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ExamplePageSink
        implements ConnectorPageSink
{
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    public ExamplePageSink(
            String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));

        // TODO: Create Pulsar producer for this schema/topic
        // see: https://pulsar.apache.org/docs/en/client-libraries-java/#producer
//        Producer<String> stringProducer = client.newProducer(Schema.STRING)
//                .topic("some-string-topic")
//                .create();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            List<Object> values = new ArrayList<>(columnTypes.size());

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                appendColumn(values, page, position, channel);
            }

            // TODO: Use ObjectMapper to encode json string for printing out
            // see: src/main/java/io/trino/plugin/kafka/encoder/json/JsonRowEncoder.java

            System.out.println(values);
        }
        return NOT_BLOCKED;
    }

    private void appendColumn(List<Object> values, Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(channel);
        if (block.isNull(position)) {
            values.add(null);
        }
        else if (BOOLEAN.equals(type)) {
            values.add(type.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            values.add(type.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            values.add(toIntExact(type.getLong(block, position)));
        }
        else if (SMALLINT.equals(type)) {
            values.add(Shorts.checkedCast(type.getLong(block, position)));
        }
        else if (TINYINT.equals(type)) {
            values.add(SignedBytes.checkedCast(type.getLong(block, position)));
        }
        else if (DOUBLE.equals(type)) {
            values.add(type.getDouble(block, position));
        }
        else if (REAL.equals(type)) {
            values.add(intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (DATE.equals(type)) {
            // TODO: support date as long?
            values.add(type.getLong(block, position));
        }
        else if (TIMESTAMP_TZ_MILLIS.equals(type)) {
            values.add(new Timestamp(unpackMillisUtc(type.getLong(block, position))));
        }
        else if (type instanceof VarcharType) {
            values.add(type.getSlice(block, position).toStringUtf8());
        }
        else if (VARBINARY.equals(type)) {
            values.add(type.getSlice(block, position).toByteBuffer());
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // TODO: Flush the publisher

        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {}
}
