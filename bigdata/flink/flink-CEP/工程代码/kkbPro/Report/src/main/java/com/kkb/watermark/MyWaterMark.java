package com.kkb.watermark;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 *
 */
public class MyWaterMark
        implements AssignerWithPeriodicWatermarks<Tuple3<Long,String,String>> {

    long currentMaxTimestamp=0L;
    final long maxOutputOfOrderness=20000L;//允许乱序时间。
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutputOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, String>
                                             element, long l) {
        Long timeStamp = element.f0;
        currentMaxTimestamp=Math.max(timeStamp,currentMaxTimestamp);
        return timeStamp;
    }
}
