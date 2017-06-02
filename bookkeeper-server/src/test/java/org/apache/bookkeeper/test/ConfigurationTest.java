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
import static org.junit.Assert.assertTrue;

import java.security.AccessControlException;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationTest {

    static {
        // this property is read when AbstractConfiguration class is loaded.
        // this test will work as expected only using a new JVM (or classloader) for the test
        System.setProperty(AbstractConfiguration.READ_SYSTEM_PROPERTIES_PROPERTY, "true");
        System.setProperty("cluster.loc", "phx.sp1.testCluster");
    }

    @Test(timeout=60000)
    public void testConfigurationOverwrite() {
        System.clearProperty("zkServers");

        ServerConfiguration conf = new ServerConfiguration();
        assertEquals(null, conf.getZkServers());

        // override setting from property
        System.setProperty("zkServers", "server1");
        // it affects previous created configurations, if the setting is not overwrite
        assertEquals("server1", conf.getZkServers());

        ServerConfiguration conf2 = new ServerConfiguration();
        assertEquals("server1", conf2.getZkServers());

        System.clearProperty("zkServers");

        // load other configuration
        ServerConfiguration newConf = new ServerConfiguration();
        assertEquals(null, newConf.getZkServers());
        newConf.setZkServers("newserver");
        assertEquals("newserver", newConf.getZkServers());
        conf2.loadConf(newConf);
        assertEquals("newserver", conf2.getZkServers());
    }

    @Test (timeout=10000)
    public void testClusterOverride() {
        ServerConfiguration conf = new ServerConfiguration();

        // Assign a specific and a generic to our environment; should get specific.
        conf.setProperty("testProp", "generic");
        conf.setProperty("phx.sp1.testCluster$testProp", "specific");

        // Assign a generic and a specific to another environment. Should get generic.
        conf.setProperty("dfw.sp1.testCluster$genericTestProp", "specific");
        conf.setProperty("genericTestProp", "generic");

        conf.setProperty("phx.sp1.testCluster$specificTestInt", 100);
        conf.setProperty("genericTestInt", 0);

        conf.setProperty("dfw.sp1.testCluster$genericTestInt", 100);
        conf.setProperty("genericTestInt", 0);

        assertTrue(conf.getString("testProp").equals("specific"));
        assertTrue(conf.getString("genericTestProp").equals("generic"));
        assertTrue(conf.getInt("genericTestInt") == 0);
        assertTrue(conf.getInt("specificTestInt") == 100);
    }

    @Test(timeout=60000)
    public void testGetZkServers() {
        System.setProperty("zkServers", "server1:port1,server2:port2");
        ServerConfiguration conf = new ServerConfiguration();
        ClientConfiguration clientConf = new ClientConfiguration();
        assertEquals("zookeeper connect string doesn't match in server configuration",
                     "server1:port1,server2:port2", conf.getZkServers());
        assertEquals("zookeeper connect string doesn't match in client configuration",
                     "server1:port1,server2:port2", clientConf.getZkServers());
    }

	@Test(timeout = 60000)
	public void testUserPermittedToStart() {
		ServerConfiguration conf = new ServerConfiguration();
		String userString = "";

		// Comma @ end
		try {
			userString = "jerrySeinfeld,elaineBennis,kramer,soupNazi, " + System.getProperty("user.name") + ",";
			conf.setPermittedStartupUsers(userString);
			conf.validateUser();
		} catch (AccessControlException ace) {
			Assert.fail("Current user is in permittedStartupUsers, but startup failed.");
		}

		// Multiple commas
		try {
			userString = "jerrySeinfeld,elaineBennis,,,kramer,,soupNazi," + System.getProperty("user.name");
			conf.setPermittedStartupUsers(userString);
			conf.validateUser();
		} catch (AccessControlException ace) {
			Assert.fail("Current user is in permittedStartupUsers, but startup failed.");
		}

		// Comma beginning
		try {
			userString = ",jerrySeinfeld,elaineBennis,kramer,soupNazi," + System.getProperty("user.name");
			conf.setPermittedStartupUsers(userString);
			conf.validateUser();
		} catch (AccessControlException ace) {
			Assert.fail("Current user is in permittedStartupUsers, but startup failed.");
		}
	}

	@Test(timeout = 60000)
	public void testUserNotPermittedToStart() {
		ServerConfiguration conf = new ServerConfiguration();
		try {
			// Comma @ end
			String userString = "jerrySeinfeld,elaineBennis,kramer,soupNazi,";
			conf.setPermittedStartupUsers(userString);
			conf.validateUser();
			Assert.fail("Current user started process but was not approved to." + "Current user: "
					+ System.getProperty("user.name") + "\t Users: " + userString);
		} catch (AccessControlException ace) {
			// Expected! Success!
		}

		try {
			// Commas in middle
			String userString = "jerrySeinfeld,,elaineBennis,kramer,,soupNazi";
			conf.setPermittedStartupUsers(userString);
			conf.validateUser();
			Assert.fail("Current user started process but was not approved to." + "Current user: "
					+ System.getProperty("user.name") + "\t Users: " + userString);
		} catch (AccessControlException ace) {
			// Expected! Success!
		}

		try {
			// Commas @ beginning
			String userString = ",jerrySeinfeld,elaineBennis,kramer,soupNazi";
			conf.setPermittedStartupUsers(userString);
			conf.validateUser();
			Assert.fail("Current user started process but was not approved to." + "Current user: "
					+ System.getProperty("user.name") + "\t Users: " + userString);
		} catch (AccessControlException ace) {
			// Expected! Success!
		}
	}

	@Test(timeout = 60000)
	public void testUserPermittedToStartEmptyConfigString() {
		try {
	        ServerConfiguration conf = new ServerConfiguration();
			conf.setPermittedStartupUsers("   ");
			conf.validateUser();
		} catch (AccessControlException ace) {
			Assert.fail("Empty permittedStartupUsers property caused user to fail BkProxyMain start.");
		}
	}
}
