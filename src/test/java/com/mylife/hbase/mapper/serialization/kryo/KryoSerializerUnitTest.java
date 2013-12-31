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

import static org.junit.Assert.assertEquals;

import java.awt.Point;

import org.junit.Test;


/**
 * unit test for the KryoSerialization
 * 
 * 
 * @author Mike E
 */

public class KryoSerializerUnitTest {

    private final Point pointExcepted = new Point(-1, -1);
    
  @Test
  public void testSerialzationDeserialiationLifeCycle() throws Exception{
      KryoSerializer kryoSerializer = new KryoSerializer();
      assertEquals(pointExcepted,kryoSerializer.deserialize(kryoSerializer.serialize(pointExcepted), Point.class)); 
  }
    
}
