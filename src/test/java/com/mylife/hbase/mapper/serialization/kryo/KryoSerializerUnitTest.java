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

package com.mylife.hbase.mapper.serialization.kryo;

import org.junit.Test;

import com.mylife.hbase.mapper.serialization.json.AbstractSerializationUnitTest;

/**
 * unit test for the KryoSerialization
 * 
 * 
 * @author Mike E
 */

public class KryoSerializerUnitTest extends AbstractSerializationUnitTest {

    final private KryoSerializer kryoSerializer = KryoSerializer.newInstance();

    @Test
    public void testNullSerialization() throws Exception {
        super.testNullSerialization(kryoSerializer);
    }

    @Test
    public void testNullDeserialization() throws Exception {
        super.testNullDeserialization(kryoSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycle() throws Exception {
        super.testSerialzationDeserialiationLifeCycle(kryoSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithList() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithList(kryoSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithMap() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithMap(kryoSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithSet() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithSet(kryoSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithQueue() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithQueue(kryoSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithCollection() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithCollection(kryoSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithWrappedArrayList() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithWrappedArrayList(kryoSerializer);
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithLinkedList() throws Exception {
        super.testSerialzationDeserialiationLifeCycleWithLinkedList(kryoSerializer);
    }
}
