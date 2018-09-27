/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.stats.codahale;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test of {@link CodahaleMetricsProvider}.
 */
@Slf4j
public class TestCodahaleMetricsProvider {

    @Test
    public void testWriteAllMetrics() throws IOException {
        @Cleanup("stop")
        StatsProvider statsProvider = new CodahaleMetricsProvider();
        testWriteAllMetricsOfProvider(statsProvider);

        statsProvider = new FastCodahaleMetricsProvider();
        testWriteAllMetricsOfProvider(statsProvider);
    }

    void testWriteAllMetricsOfProvider(StatsProvider statsProvider) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final String statsLoggerName = "testStatsLoggerName";
        final String opStatsLoggerName = "testOpStatsLoggerName";
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty("jettyPort", new Integer(0));

        statsProvider.start(config);
        OpStatsLogger logger = statsProvider.getStatsLogger(statsLoggerName).getOpStatsLogger(opStatsLoggerName);
        logger.registerSuccessfulValue(1);
        StringWriter writer = new StringWriter();
        statsProvider.writeAllMetrics(writer);
        String metricsJsonString = writer.toString();
        log.info(metricsJsonString);

        JsonNode jsonNode = mapper.readTree(metricsJsonString);
        List<JsonNode> statsLoggerList = jsonNode.findValues(statsLoggerName + "." + opStatsLoggerName);
        Assert.assertTrue("there should be json node for statsLogger", statsLoggerList.size() >= 1);
    }
}
