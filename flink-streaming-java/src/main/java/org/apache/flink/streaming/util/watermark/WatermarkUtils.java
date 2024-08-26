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

package org.apache.flink.streaming.util.watermark;

import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.api.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.watermark.WatermarkDeclaration;
import org.apache.flink.datastream.watermark.DeclarableWatermark;
import org.apache.flink.runtime.watermark.InternalLongWatermarkDeclaration;
import org.apache.flink.runtime.watermark.InternalWatermarkDeclaration;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WatermarkUtils {

    public static Collection<? extends WatermarkDeclaration> getWatermarkDeclarations(
            StreamOperator<?> streamOperator) {
        return (streamOperator instanceof DeclarableWatermark)
                ? ((DeclarableWatermark) streamOperator).watermarkDeclarations()
                : Collections.emptySet();
    }

    public static InternalWatermarkDeclaration convertToInternalWatermarkDeclaration(
            WatermarkDeclaration watermarkDeclaration) {
        if (watermarkDeclaration instanceof LongWatermarkDeclaration) {
            return new InternalLongWatermarkDeclaration(watermarkDeclaration.getIdentifier());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported watermark declaration: " + watermarkDeclaration);
        }
    }

    public static Map<
                    Class<? extends GeneralizedWatermark>,
                    InternalWatermarkDeclaration.WatermarkCombiner>
            deriveWatermarkCombinerMap(StreamConfig streamConfig, ClassLoader userClassloader) {
        Set<InternalWatermarkDeclaration> watermarkDeclarations =
                streamConfig.getWatermarkDeclarations(userClassloader);
        Map<Class<? extends GeneralizedWatermark>, InternalWatermarkDeclaration.WatermarkCombiner>
                watermarkCombinerMap = new HashMap<>();
        for (InternalWatermarkDeclaration watermarkDeclaration : watermarkDeclarations) {
            watermarkDeclaration
                    .watermarkCombiner()
                    .ifPresent(
                            combiner -> {
                                watermarkCombinerMap.put(
                                        watermarkDeclaration.declaredWatermark().watermarkClass(),
                                        combiner);
                            });
        }
        return watermarkCombinerMap;
    }
}
