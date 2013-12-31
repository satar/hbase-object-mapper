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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.List;

import javax.mail.internet.ContentType;
import javax.swing.GroupLayout.Alignment;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

/**
 * stand alone unit test for the type converter
 * 
 * 
 * @author Mike E
 */

public class TypeHandlerUnitTest {

    private final static Long ONE = 1l;
    private final static Integer TWO = 1;
    private final static Short THREE = 1;
    private final static Double FOUR = 4.0;
    private final static Float FIVE = 5.0f;
    private final static String HI = "hi";
    private final static Boolean FALSE = false;
    private final static byte[] BYTE_ARRAY = new byte[] { 3 };
    private final static ContentType CONTENT_TYPE = (ContentType) Whitebox.getInternalState(TypeHandler.class,
            "DEFAULT_CONTENT_TYPE");

    @Test
    public void getTypedValueTest() throws Exception {
        assertEquals(ONE, TypeHandler.getTypedValue(Long.class, Bytes.toBytes(ONE)));
        assertEquals(TWO, TypeHandler.getTypedValue(Integer.class, Bytes.toBytes(TWO)));
        assertEquals(THREE, TypeHandler.getTypedValue(Short.class, Bytes.toBytes(THREE)));
        assertEquals(FOUR, TypeHandler.getTypedValue(Double.class, Bytes.toBytes(FOUR)));
        assertEquals(FIVE, TypeHandler.getTypedValue(Float.class, Bytes.toBytes(FIVE)));
        assertEquals(HI, TypeHandler.getTypedValue(String.class, Bytes.toBytes(HI)));
        assertEquals(FALSE, TypeHandler.getTypedValue(Boolean.class, Bytes.toBytes(FALSE)));
        assertEquals(BYTE_ARRAY, TypeHandler.getTypedValue(byte[].class, BYTE_ARRAY));
        assertEquals(Alignment.BASELINE,
                TypeHandler.getTypedValue(Alignment.class, Bytes.toBytes(Alignment.BASELINE.name())));
        assertEquals(CONTENT_TYPE.toString(),
                TypeHandler.getTypedValue(ContentType.class, Bytes.toBytes(CONTENT_TYPE.toString())).toString());
        assertNull(TypeHandler.getTypedValue(null, null));
    }
    
    @Test
    public void getTypeSupportTest() throws Exception {
        assertTrue(TypeHandler.supports(Long.class));
        assertTrue(TypeHandler.supports(Integer.class));
        assertTrue(TypeHandler.supports(Short.class));
        assertTrue(TypeHandler.supports(Double.class));
        assertTrue(TypeHandler.supports(Float.class));
        assertTrue(TypeHandler.supports(String.class));
        assertTrue(TypeHandler.supports(Boolean.class));
        assertTrue(TypeHandler.supports(byte[].class));
        assertTrue(TypeHandler.supports(Alignment.class));
        assertTrue(TypeHandler.supports(ContentType.class));
        assertFalse(TypeHandler.supports(List.class));
    }

    @Test
    public void getTypedValueBadContentTypeTest() throws Exception {
        // should return default value
        assertEquals(CONTENT_TYPE.toString(),
                TypeHandler.getTypedValue(ContentType.class, Bytes.toBytes("Bad content type")).toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getTypedValueUnsupportedType() throws Exception {
        TypeHandler.getTypedValue(BigDecimal.class, Bytes.toBytes(ONE));
    }
}
