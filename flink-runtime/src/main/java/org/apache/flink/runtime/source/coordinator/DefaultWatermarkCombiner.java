/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkCombiner;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class DefaultWatermarkCombiner implements WatermarkCombiner {

    protected final Map<Integer, SourceCoordinator.WatermarkElement> watermarks = new LinkedHashMap<>();

    private final HeapPriorityQueue<SourceCoordinator.WatermarkElement> orderedWatermarks =
            new HeapPriorityQueue<>(PriorityComparator.forPriorityComparableObjects(), 10);

    private static final Watermark DEFAULT_WATERMARK = new Watermark(Long.MIN_VALUE);


    @Override
    public Watermark combineWatermark(Watermark watermark, Context context) {
        SourceCoordinator.WatermarkElement watermarkElement = new SourceCoordinator.WatermarkElement(watermark);
        SourceCoordinator.WatermarkElement oldWatermarkElement = watermarks.put(context.getIndexOfCurrentChannel(), watermarkElement);
        if (oldWatermarkElement != null) {
            orderedWatermarks.remove(oldWatermarkElement);
        }
        orderedWatermarks.add(watermarkElement);

        return getAggregatedWatermark();
    }

    @Override
    public Watermark getAggregatedWatermark() {
        SourceCoordinator.WatermarkElement aggregatedWatermarkElement = orderedWatermarks.peek();
        return aggregatedWatermarkElement == null
                ? DEFAULT_WATERMARK
                : aggregatedWatermarkElement.getWatermark();
    }
}
