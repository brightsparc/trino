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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJdbcMetadataConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JdbcMetadataConfig.class)
                .setAllowDropTable(false)
                .setJoinPushdownEnabled(false)
                .setAggregationPushdownEnabled(true)
                .setTopNPushdownEnabled(true)
                .setDomainCompactionThreshold(32)
                .setWriteBatchSize(1000)
                .setNonTransactionalInsert(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("allow-drop-table", "true")
                .put("join-pushdown.enabled", "true")
                .put("aggregation-pushdown.enabled", "false")
                .put("domain-compaction-threshold", "42")
                .put("topn-pushdown.enabled", "false")
                .put("write.batch-size", "24")
                .put("insert.non-transactional-insert.enabled", "true")
                .build();

        JdbcMetadataConfig expected = new JdbcMetadataConfig()
                .setAllowDropTable(true)
                .setJoinPushdownEnabled(true)
                .setAggregationPushdownEnabled(false)
                .setTopNPushdownEnabled(false)
                .setDomainCompactionThreshold(42)
                .setWriteBatchSize(24)
                .setNonTransactionalInsert(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testWriteBatchSizeValidation()
    {
        assertThatThrownBy(() -> makeConfig(ImmutableMap.of("write.batch-size", "-42")))
                .hasMessageContaining("write.batch-size: must be greater than or equal to 1");

        assertThatThrownBy(() -> makeConfig(ImmutableMap.of("write.batch-size", "0")))
                .hasMessageContaining("write.batch-size: must be greater than or equal to 1");

        assertThatCode(() -> makeConfig(ImmutableMap.of("write.batch-size", "1")))
                .doesNotThrowAnyException();

        assertThatCode(() -> makeConfig(ImmutableMap.of("write.batch-size", "42")))
                .doesNotThrowAnyException();
    }

    private static JdbcMetadataConfig makeConfig(Map<String, String> props)
    {
        return new ConfigurationFactory(props).build(JdbcMetadataConfig.class);
    }
}
