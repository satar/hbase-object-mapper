// =======================================================
// Copyright Mylife.com Inc., 2013. All rights reserved.
//
// =======================================================
//  Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.mylife.hbase.mapper.serialization.json;

import org.junit.Test;

/**
 * unit test for the JsonSerialization
 * 
 * 
 * @author Mike E
 */

public class JsonSerializerUnitTest extends AbstractSerializationUnitTest {

    final private JsonSerializer jsonSerializer = JsonSerializer.newInstance();

    @Test
    public void testNullSerialization() throws Exception {
        super.testNullSerialization(jsonSerializer);
    }

    @Test
    public void testNullDeserialization() throws Exception {
        super.testNullDeserialization(jsonSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycle() throws Exception {
        super.testSerialzationDeserialiationLifeCycle(jsonSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithList() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithList(jsonSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithMap() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithMap(jsonSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithSet() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithSet(jsonSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithQueue() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithQueue(jsonSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithCollection() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithCollection(jsonSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithWrappedArrayList() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithWrappedArrayList(jsonSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithLinkedList() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithLinkedList(jsonSerializer);
    }
}
