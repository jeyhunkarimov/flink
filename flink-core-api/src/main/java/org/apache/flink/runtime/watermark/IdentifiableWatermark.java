package org.apache.flink.runtime.watermark;

import java.io.Serializable;

public interface IdentifiableWatermark extends Serializable {
    String getIdentifier();
}
