/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.timer.TimerTask;

/**
 * RedeliveryDelayTimerTask is a timer task that is executed when the redelivery delay of records expires.
 * Records which are made available again, by a release, an acquisition lock timeout or a consumer
 * disconnect, are not acquirable until the task expires. A single task can be shared by the per-offset
 * states of contiguous records.
 */
public class RedeliveryDelayTimerTask extends TimerTask {

    private final long expirationMs;
    private final long firstOffset;
    private final long lastOffset;
    private final RedeliveryDelayHandler handler;

    public RedeliveryDelayTimerTask(
        Time time,
        long delayMs,
        long firstOffset,
        long lastOffset,
        RedeliveryDelayHandler handler
    ) {
        super(delayMs);
        this.expirationMs = time.hiResClockMs() + delayMs;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.handler = handler;
    }

    /**
     * @return the time in milliseconds, as per {@link Time#hiResClockMs()}, at which the delay expires.
     */
    public long expirationMs() {
        return expirationMs;
    }

    /**
     * @return the first offset of the records delayed by this task.
     */
    public long firstOffset() {
        return firstOffset;
    }

    /**
     * @return the last offset of the records delayed by this task.
     */
    public long lastOffset() {
        return lastOffset;
    }

    /**
     * The task is executed when the redelivery delay expires. The task makes the delayed records
     * available for redelivery.
     */
    @Override
    public void run() {
        handler.handle(firstOffset, lastOffset, this);
    }
}
