/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.datastream.impl.utils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.types.Value;

import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TypeDescriptorUtils}. */
class TypeDescriptionUtilsTest {

    public static class MockPojo {
        public Double aDouble;
        public Float aFloat;
    }

    @Test
    void testObjectArrayTypeDescriptor() {
        TypeDescriptors.TypeDescriptor<Integer> integerTypeDescriptor = () -> Integer.class;
        TypeDescriptors.ObjectArrayTypeDescriptor<Object[], Integer> descriptor =
                new TypeDescriptors.ObjectArrayTypeDescriptor<Object[], Integer>() {
                    @Override
                    public TypeDescriptors.TypeDescriptor<Integer> getComponentInfo() {
                        return integerTypeDescriptor;
                    }

                    @Override
                    public Class<Object[]> getTypeClass() {
                        return Object[].class;
                    }
                };
        assertThat(TypeDescriptorUtils.buildTypeInfo(descriptor))
                .isInstanceOf(ObjectArrayTypeInfo.class);
    }

    @Test
    void testBasicTypeDescriptor() {
        TypeDescriptors.BasicTypeDescriptor<Long> descriptor = () -> Long.class;
        assertThat(TypeDescriptorUtils.buildTypeInfo(descriptor)).isInstanceOf(BasicTypeInfo.class);
    }

    @Test
    void testBasicArrayTypeDescriptor() {
        TypeDescriptors.BasicArrayTypeDescriptor<String[]> descriptor = () -> String[].class;
        assertThat(TypeDescriptorUtils.buildTypeInfo(descriptor))
                .isInstanceOf(BasicArrayTypeInfo.class);
    }

    @Test
    void testListTypeDescriptor() {
        TypeDescriptors.ListTypeDescriptor<Boolean> descriptor = () -> Boolean.class;
        assertThat(TypeDescriptorUtils.buildTypeInfo(descriptor)).isInstanceOf(ListTypeInfo.class);
    }

    @Test
    void testValueTypeDescriptor() {
        TypeDescriptors.ValueTypeDescriptor<Value> descriptor = () -> Value.class;
        assertThat(TypeDescriptorUtils.buildTypeInfo(descriptor)).isInstanceOf(ValueTypeInfo.class);
    }

    @Test
    void testSqlTimeTypeDescriptor() {
        TypeDescriptors.SqlTimeTypeDescriptor<Time> descriptor = () -> Time.class;
        assertThat(TypeDescriptorUtils.buildTypeInfo(descriptor))
                .isInstanceOf(SqlTimeTypeInfo.class);
    }

    @Test
    void testPrimitiveArrayTypeDescriptor() {
        TypeDescriptors.PrimitiveArrayTypeDescriptor<int[]> descriptor = () -> int[].class;
        assertThat(TypeDescriptorUtils.buildTypeInfo(descriptor))
                .isInstanceOf(PrimitiveArrayTypeInfo.class);
    }

    @Test
    void testPojoTypeDescriptor() {

        TypeDescriptors.BasicTypeDescriptor<Double> integerTypeDescriptor = () -> Double.class;
        TypeDescriptors.BasicTypeDescriptor<Float> floatTypeDescriptor = () -> Float.class;

        TypeDescriptors.PojoTypeDescriptor<MockPojo> descriptor =
                new TypeDescriptors.PojoTypeDescriptor<MockPojo>() {
                    @Override
                    public Map<String, TypeDescriptors.TypeDescriptor<?>> getFields() {
                        Map<String, TypeDescriptors.TypeDescriptor<?>> map = new HashMap<>();
                        map.put("aDouble", integerTypeDescriptor);
                        map.put("aFloat", floatTypeDescriptor);
                        return map;
                    }

                    @Override
                    public Class<MockPojo> getTypeClass() {
                        return MockPojo.class;
                    }
                };
        assertThat(TypeDescriptorUtils.buildTypeInfo(descriptor)).isInstanceOf(PojoTypeInfo.class);
    }
}
