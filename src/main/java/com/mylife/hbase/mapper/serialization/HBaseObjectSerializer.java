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
package com.mylife.hbase.mapper.serialization;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * interface to abstract out serialize to allow for selectable serialization
 *
 * @author Mike E.
 */
public interface HBaseObjectSerializer {

    byte[] serialize(Object object) throws IOException;
    
    public <T> T deserialize(byte[] byteArray, Field field) throws IOException;
}
