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

package com.mylife.hbase.mapper.util;

import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * type mappings
 * 
 * @author Mike E
 */

public final class TypeHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TypeHandler.class);

    private static final ContentType DEFAULT_CONTENT_TYPE = getTextPlainContentType();

    private static ContentType getTextPlainContentType() {
        try {
            return new ContentType("text/plain; charset=UTF-8");
        } catch (ParseException e) {
            // Should never happen
            throw new RuntimeException(e);
        }
    }

    private TypeHandler() {
        super();
    }

    /**
     * 
     * @param type
     * @return returns turn if the given type is currently supported
     */
    public static boolean supports(@SuppressWarnings("rawtypes") final Class type) {
        if (type == Boolean.class || type == boolean.class || type.isEnum()) {
            return true;
        }
        try {
            getTypedValue(type, new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getTypedValue(@SuppressWarnings("rawtypes") final Class type, final byte[] value) {
        if (value == null) {
            return null;
        } else if (type == Integer.class || type == int.class) {
            return (T) Integer.valueOf(Bytes.toInt(value));
        } else if (type == Long.class || type == long.class) {
            return (T) Long.valueOf(Bytes.toLong(value));
        } else if (type == String.class) {
            return (T) Bytes.toString(value);
        } else if (type == Boolean.class || type == boolean.class) {
            return (T) Boolean.valueOf(Bytes.toBoolean(value));
        } else if (type == Float.class || type == float.class) {
            return (T) Float.valueOf(Bytes.toFloat(value));
        } else if (type == Double.class || type == double.class) {
            return (T) Double.valueOf(Bytes.toDouble(value));
        } else if (type == Short.class || type == short.class) {
            return (T) Short.valueOf(Bytes.toShort(value));
        } else if (type == byte[].class) {
            return (T) value;
        } else if (type.isEnum()) {
            return (T) Enum.valueOf(type, Bytes.toString(value));
        } else if (type == ContentType.class) {
            try {
                return (T) new ContentType(Bytes.toString(value));
            } catch (ParseException e) {
                LOG.warn("Invalid content type retrieved.", e);
            }
            return (T) DEFAULT_CONTENT_TYPE;
        } else {
            throw new IllegalArgumentException("Unknow type: " + type + " please add handling for this type");
        }
    }

}
