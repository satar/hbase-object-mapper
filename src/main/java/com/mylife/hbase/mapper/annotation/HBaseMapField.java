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

package com.mylife.hbase.mapper.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation to show that this field is a map and that the contents
 * should be stored in HBase as individual qualifiers. Using the map keys as
 * qualifier names (as a java.lang.String) and the corresponding values as the
 * values. This allows for dynamic map backed objects that can utilize the
 * flexible schema nature of HBase Fields marked with the annotation should
 * implement java.util.Map<String,String>. Must have a corresponding getter.
 * 
 * @author MikeE
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseMapField {
    String columnFamilyName() default "";
}
