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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Configuration parameters for watermark alignment. */
@PublicEvolving
public interface WatermarkCombiner extends Serializable {

    /** Input channel has received a watermark. */
    Watermark combineWatermark(Watermark watermark, Context context);

    Watermark getAggregatedWatermark();

    /** This interface provides information of all channels. */
    interface Context {
        int getNumberOfInputChannels();
        int getIndexOfCurrentChannel();

        boolean isSubtaskContext();
    }

    static Context createContext(int numberOfInputChannels, int indexOfCurrentChannel, boolean isSubtaskContext) {
        return new Context() {
            @Override
            public int getNumberOfInputChannels() {
                return numberOfInputChannels;
            }

            @Override
            public int getIndexOfCurrentChannel() {
                return indexOfCurrentChannel;
            }

            @Override
            public boolean isSubtaskContext() {
                return isSubtaskContext;
            }
        };
    }
}
