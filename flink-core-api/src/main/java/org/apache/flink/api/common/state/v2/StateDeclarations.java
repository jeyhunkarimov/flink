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

package org.apache.flink.api.common.state.v2;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeDescriptor;

/** This is a helper class for declaring various states. */
public class StateDeclarations {
    /** Get the builder of {@link ListStateDeclaration}. */
    public static <T> ListStateDeclarationBuilder<T> listStateBuilder(
            String name, TypeDescriptor<T> elementTypeInformation) {
        return null;
    }

    /** Get the builder of {@link MapStateDeclaration}. */
    public static <K, V> MapStateDeclarationBuilder<K, V> mapStateBuilder(
            String name,
            TypeDescriptor<K> keyTypeInformation,
            TypeDescriptor<V> valueTypeInformation) {
        return null;
    }

    /** Get the builder of {@link ValueStateDeclaration}. */
    public <T> ValueStateDeclarationBuilder<T> valueStateBuilder(
            String name, TypeDescriptor<T> valueType) {
        return null;
    }

    /** Get the builder of {@link ReducingStateDeclaration}. */
    public <T> ReducingStateDeclarationBuilder<T> reducingStateBuilder(
            String name, TypeDescriptor<T> typeInformation) {
        return null;
    }

    /** Get the builder of {@link AggregatingStateDeclaration}. */
    public <IN, OUT, ACC> AggregatingStateDeclarationBuilder<IN, OUT, ACC> aggregatingStateBuilder(
            String name, AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        return null;
    }

    /**
     * Get the {@link ListStateDeclaration} of list state with {@link
     * StateDeclaration.RedistributionMode#NONE}. If you want to configure it more elaborately, use
     * {@link StateDeclarations#listStateBuilder(String, TypeDescriptor)}.
     */
    public static <T> ListStateDeclaration<T> listState(
            String name, TypeDescriptor<T> elementTypeInformation) {
        return null;
    }

    /**
     * Get the {@link MapStateDeclaration} of map state with {@link
     * StateDeclaration.RedistributionMode#NONE}. If you want to configure it more elaborately, use
     * {@link StateDeclarations#mapStateBuilder(String, TypeDescriptor, TypeDescriptor)}.
     */
    public static <K, V> MapStateDeclaration<K, V> mapState(
            String name,
            TypeDescriptor<K> keyTypeInformation,
            TypeDescriptor<V> valueTypeInformation) {
        return null;
    }

    /**
     * Get the {@link ValueStateDeclaration} of value state. If you want to configure it more
     * elaborately, use {@link StateDeclarations#valueStateBuilder(String, TypeDescriptor)}.
     */
    public static <T> ValueStateDeclaration<T> valueState(
            String name, TypeDescriptor<T> valueType) {
        return null;
    }

    /**
     * Get the {@link ReducingStateDeclaration} of list state. If you want to configure it more
     * elaborately, use {@link StateDeclarations#reducingStateBuilder(String, TypeDescriptor)}.
     */
    public static <T> ReducingStateDeclaration<T> reducingState(
            String name, TypeDescriptor<T> typeInformation) {
        return null;
    }

    /**
     * Get the {@link AggregatingStateDeclaration} of aggregating state. If you want to configure it
     * more elaborately, use {@link #aggregatingStateBuilder(String, AggregateFunction)}.
     */
    public static <IN, OUT, ACC> AggregatingStateDeclaration<IN, OUT, ACC> aggregatingState(
            String name, AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        return null;
    }

    /** Builder for {@link ListStateDeclaration}. */
    public static class ListStateDeclarationBuilder<T> {

        ListStateDeclarationBuilder<T> redistributeBy(
                ListStateDeclaration.RedistributionStrategy strategy) {
            return null;
        }

        ListStateDeclaration<T> build() {
            return null;
        }
    }

    /** Builder for {@link MapStateDeclaration}. */
    public static class MapStateDeclarationBuilder<K, V> {
        MapStateDeclarationBuilder<K, V> broadcast() {
            return null;
        }

        MapStateDeclaration<K, V> build() {
            return null;
        }
    }

    /** Builder for {@link ValueStateDeclaration}. */
    public static class ValueStateDeclarationBuilder<T> {
        ValueStateDeclaration<T> build() {
            return null;
        }
    }

    /** Builder for {@link ReducingStateDeclaration}. */
    public static class ReducingStateDeclarationBuilder<T> {
        ReducingStateDeclaration<T> build() {
            return null;
        }
    }

    /** Builder for {@link AggregatingStateDeclaration}. */
    public static class AggregatingStateDeclarationBuilder<IN, OUT, ACC> {
        AggregatingStateDeclaration<IN, OUT, ACC> build() {
            return null;
        }
    }
}
