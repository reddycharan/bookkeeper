/*
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
package org.apache.bookkeeper.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interprets Rest server configs.
 */
@Path("/resources")
public class RestResources {

    private static final Logger LOG = LoggerFactory.getLogger(RestResources.class);

    private static AbstractConfiguration conf;
    private static ObjectMapper om = new ObjectMapper();

    public static void setServerConfiguration(AbstractConfiguration conf) {
        RestResources.conf = conf;
    }

    @SuppressWarnings("unchecked")
    @GET
    @Path("/v1/configurations")
    @Produces(MediaType.APPLICATION_JSON)
    public String getConfigurationV1(@QueryParam("config") String configName) {
        Map<String, String> configs = new HashMap<>();
        try {
            if (!Strings.isNullOrEmpty(configName)) {
                if (conf.containsKey(configName)) {
                    configs.put(configName, conf.getString(configName));
                    return om.writeValueAsString(configs);
                } else {
                    return "{ \"error \" : \"no such config \" }";
                }
            }
            return conf.asJson();
        } catch (JsonProcessingException e) {
            if (!Strings.isNullOrEmpty(configName)) {
                LOG.error("Error processing JSON for configName: " + configName, e);
            } else {
                LOG.error("Error processing JSON config for all properties: ", e);
            }
            return "{ \"error\" : \"" + e.getMessage() + "\"";
        } catch (Exception e) {
            if (!Strings.isNullOrEmpty(configName)) {
            LOG.error("Couldn't get configName: " + configName , e);
            } else {
                LOG.error("Couldn't retrieve /v1/configurations for all configs", e);
            }
            return "{ \"error\" : \"" + e.getMessage() + "\"";
        }
    }

}
