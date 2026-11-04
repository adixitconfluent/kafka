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

/**
 * RedeliveryDelayHandler is an interface that defines a handler for the expiry of a redelivery delay.
 * It is used to make records available for redelivery once their redelivery delay has elapsed.
 */
public interface RedeliveryDelayHandler {

    /**
     * Handles the expiry of the redelivery delay for a range of offsets of a share partition.
     *
     * @param firstOffset the first offset
     * @param lastOffset the last offset
     * @param timerTask the timer task which expired
     */
    void handle(long firstOffset, long lastOffset, RedeliveryDelayTimerTask timerTask);

}
