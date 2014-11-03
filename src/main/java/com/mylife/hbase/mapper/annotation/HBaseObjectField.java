// =======================================================
// Copyright Mylife.com Inc., 2014. All rights reserved.
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

package com.mylife.hbase.mapper.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.esotericsoftware.kryo.Kryo;
import com.mylife.hbase.mapper.SerializationStategy;

/**
 * Marker annotation to show that this field should serialized and stored in HBase as a blob.
 * 
 * This serialization/de-serialization is done via {@link Kryo} with {@link Kryo#setDefaultSerializer() }
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseObjectField {
    String columnFamilyName() default "";

    /*
     * Flag that can be set for use with a coprocessor to determine if index should be created from this field.
     */
    boolean indexable() default false;

    SerializationStategy serializationStategy() default SerializationStategy.KRYO;
}
