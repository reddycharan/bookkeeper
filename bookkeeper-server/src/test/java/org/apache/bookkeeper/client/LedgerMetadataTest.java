/*
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
 */

package org.apache.bookkeeper.client;

import static org.junit.Assert.*;
import static com.google.common.base.Charsets.UTF_8;

import java.util.Collections;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.junit.Test;

/**
 * Unit test for ledger metadata.
 */
public class LedgerMetadataTest {

    private static final String passwdStr = "testPasswd";
    private static final byte[] passwd = passwdStr.getBytes(UTF_8);
    
    @Test
    public void testToString() {
        LedgerMetadata lm1 = new LedgerMetadata(
            3,
            3,
            2,
            DigestType.CRC32,
            passwd,
            Collections.emptyMap());

        assertTrue("toString should contain 'password' field", lm1.toString().contains("password"));
        assertTrue("toString should contain password value", lm1.toString().contains(passwdStr));
        assertFalse("toSafeString should not contain 'password' field", lm1.toSafeString().contains("password"));
        assertFalse("toSafeString should not contain password value", lm1.toSafeString().contains(passwdStr));
    }
}
