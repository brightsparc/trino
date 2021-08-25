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

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class ExampleTableProperties
{
    public static final String TOPIC_FORMAT_PROPERTY = "format";
    public static final String PARTITION_KEY_PROPERTY = "partition_key";
    public static final String EVENT_TIMESTAMP_PROPERTY = "event_timestamp";
    public static final String SCHEMA_COMPATIBILITY_STRATEGY_PROPERTY = "schema_compatability_strategy";

    private final List<PropertyMetadata<?>> tableProperties;

    public ExampleTableProperties()
    {
        // TODO: Provide a list of property metadata
        tableProperties = ImmutableList.of(
                enumProperty(
                        TOPIC_FORMAT_PROPERTY,
                        "Topic format for messages",
                        TopicFormat.class,
                        TopicFormat.JSON,
                        false),
                stringProperty(PARTITION_KEY_PROPERTY, "Partition key column", null, false),
                stringProperty(EVENT_TIMESTAMP_PROPERTY, "Event timestamp column", null, false),
                enumProperty(
                        SCHEMA_COMPATIBILITY_STRATEGY_PROPERTY,
                        "Schema compatibility strategy",
                        SchemaCompatibilityStrategy.class,
                        SchemaCompatibilityStrategy.UNDEFINED,
                        false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public enum TopicFormat
    {
        AVRO("AVRO"),
        JSON("JSON"),
        PROTOBUF("PROTOBUF");

        private final String format;

        TopicFormat(String format)
        {
            this.format = requireNonNull(format, "format is null");
        }
    }
}
