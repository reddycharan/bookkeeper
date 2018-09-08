/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.JsonUtil;
import org.apache.bookkeeper.util.JsonUtil.ParseJsonException;
import org.junit.Test;

/**
 * Test the configuration class.
 */
public class ConfigurationTest {

    static {
        // this property is read when AbstractConfiguration class is loaded.
        // this test will work as expected only using a new JVM (or classloader) for the test
        System.setProperty(AbstractConfiguration.READ_SYSTEM_PROPERTIES_PROPERTY, "true");
        System.setProperty(AbstractConfiguration.CLUSTER_LOC_PROPERTY, "phx.sp1.testCluster");
    }

    @Test
    public void testConfigurationOverwrite() {
        System.clearProperty("metadataServiceUri");

        ServerConfiguration conf = new ServerConfiguration();
        assertEquals(null, conf.getMetadataServiceUriUnchecked());

        // override setting from property
        System.setProperty("metadataServiceUri", "zk://server:2181/ledgers");
        // it affects previous created configurations, if the setting is not overwrite
        assertEquals("zk://server:2181/ledgers", conf.getMetadataServiceUriUnchecked());

        ServerConfiguration conf2 = new ServerConfiguration();
        assertEquals("zk://server:2181/ledgers", conf2.getMetadataServiceUriUnchecked());

        System.clearProperty("metadataServiceUri");

        // load other configuration
        ServerConfiguration newConf = new ServerConfiguration();
        assertEquals(null, newConf.getMetadataServiceUriUnchecked());
        newConf.setMetadataServiceUri("zk://newserver:2181/ledgers");
        assertEquals("zk://newserver:2181/ledgers", newConf.getMetadataServiceUriUnchecked());
        conf2.loadConf(newConf);
        assertEquals("zk://newserver:2181/ledgers", conf2.getMetadataServiceUriUnchecked());
    }

    @Test
    public void testClusterOverride() throws ParseJsonException {
        ServerConfiguration conf = new ServerConfiguration();

        // Assign a value to our environment; should get same value with and
        // without prefix.
        conf.setProperty("testProp", "specific");
        conf.setProperty("genericTestInt", 100);
        conf.setPropertyUnPrefixed("simpleconfig", "simplevalue");
        conf.setPropertyUnPrefixed("dfw.sp2.testCluster$dfwconfig", "dfwvalue");
        conf.setPropertyUnPrefixed("phx.sp1.testCluster$newconfig", "newconfigvalue");
        conf.setPropertyUnPrefixed("overriddenconfig", "overriddenbasevalue");
        conf.setPropertyUnPrefixed("phx.sp1.testCluster$overriddenconfig", "overriddenconfigvalue");

        assertTrue(conf.getString("testProp").equals("specific"));
        assertTrue(conf.getString("phx.sp1.testCluster$testProp").equals("specific"));
        assertTrue(conf.getInt("genericTestInt") == 100);
        assertTrue(conf.getInt("phx.sp1.testCluster$genericTestInt") == 100);

        @SuppressWarnings("unchecked")
        HashMap<String, String> confAsMap = JsonUtil.fromJson(conf.asJson(), HashMap.class);
        /*
         * since READ_SYSTEM_PROPERTIES_PROPERTY is set, conf should contain
         * system properties.
         *
         */
        assertEquals("CLUSTER_LOC_PROPERTY", "phx.sp1.testCluster",
                confAsMap.get(AbstractConfiguration.CLUSTER_LOC_PROPERTY));
        assertFalse("CLUSTER_LOC_PROPERTY with SEPARATOR", confAsMap
                .containsKey(AbstractConfiguration.CLUSTER_LOC_PROPERTY + AbstractConfiguration.CLUSTER_SEPARATOR));
        /*
         * should be able to get this cluster config values.
         */
        assertEquals("simpleconfig property", "simplevalue", confAsMap.get("simpleconfig"));
        assertEquals("newconfig property", "newconfigvalue", confAsMap.get("newconfig"));
        assertEquals("overriddenconfig config should be overridden value", "overriddenconfigvalue",
                confAsMap.get("overriddenconfig"));
        /*
         * since conf.asJson() is only going to contain trimmed config keys of
         * this cluster, all other config properties should be removed from
         * returned conf.asJson().
         */
        assertFalse("phx.sp1.testCluster$newconfig property", confAsMap.containsKey("phx.sp1.testCluster$newconfig"));
        assertFalse("dfwconfig property", confAsMap.containsKey("dfwconfig"));
        assertFalse("dfw.sp2.testCluster$dfwconfig property", confAsMap.containsKey("dfw.sp2.testCluster$dfwconfig"));

        // GetStringArray should also work
        String arr[] = { "v1", "v2", "v3" };
        conf.setProperty("testList", "v1,v2,v3");
        assertTrue(Arrays.equals(conf.getStringArray("testList"), arr));
        assertTrue(Arrays.equals(conf.getStringArray("phx.sp1.testCluster$testList"), arr));
    }

    @Test
    public void testGetZkServers() {
        System.setProperty("metadataServiceUri", "zk://server1:port1;server2:port2/ledgers");
        ServerConfiguration conf = new ServerConfiguration();
        ClientConfiguration clientConf = new ClientConfiguration();
        assertEquals("zookeeper connect string doesn't match in server configuration",
                     "zk://server1:port1;server2:port2/ledgers", conf.getMetadataServiceUriUnchecked());
        assertEquals("zookeeper connect string doesn't match in client configuration",
                     "zk://server1:port1;server2:port2/ledgers", clientConf.getMetadataServiceUriUnchecked());
    }
}
