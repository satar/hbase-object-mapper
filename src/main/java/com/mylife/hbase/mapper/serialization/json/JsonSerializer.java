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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mylife.hbase.mapper.serialization.HBaseObjectSerializer;
import com.mylife.hbase.mapper.util.TypeHandler;

public class JsonSerializer implements HBaseObjectSerializer {

    private static final ObjectMapper OBJECT_MAPPER = GET_OBJECT_MAPPER();
    
    private static ObjectMapper GET_OBJECT_MAPPER(){
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        return objectMapper;
    }
    
    private JsonSerializer(){
        
    }
    
    public static final JsonSerializer newInstance(){
        return new JsonSerializer(); 
    }

    @Override
    public byte[] serialize(final Object object) throws IOException {
        if(object == null){
            return null;
        }
        return OBJECT_MAPPER.writeValueAsBytes(object);
    }

    @Override
    public <T> T deserialize(final byte[] byteArray, final Field field) throws IOException {
        if(byteArray == null || field == null){
            return null;
        }
        @SuppressWarnings("unchecked")
        final Class<T> type = (Class<T>) field.getType();
        if(type.isAssignableFrom(Map.class)){
            final Type[] types = TypeHandler.getGenericTypesFromField(field);
            return OBJECT_MAPPER.reader(OBJECT_MAPPER.getTypeFactory().constructMapType(
                    HashMap.class, (Class<?>) types[0], (Class<?>) types[1])).readValue(byteArray);
        }
        return OBJECT_MAPPER.readValue(byteArray, type);
    }

}
