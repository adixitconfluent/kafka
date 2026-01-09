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
package org.apache.kafka.server.share.fetch.acquire;

import java.util.Optional;

/**
 * Tracks and manages gaps from persister read results during acquisition.
 * <p>
 * When SharePartition is initialized from persister state, there may be gaps
 * (offsets not tracked by either persister or cached state) that need to be
 * acquired. This class encapsulates all gap-tracking logic to simplify the
 * acquire method.
 * <p>
 * The gap window represents a range where gaps may exist:
 * <ul>
 *   <li>{@code endOffset} - The fixed end boundary of the gap window</li>
 *   <li>{@code gapStartOffset} - The current start of unexplored gaps (advances as gaps are acquired)</li>
 * </ul>
 * <p>
 * Example: If persister returns batches at [0-9, 20-29] with endOffset=29,
 * there's a potential gap at [10-19]. The gapStartOffset starts at 10 and
 * advances to 20 once the gap is acquired.
 */
public class PersisterGapTracker {

    private final long endOffset;
    private long gapStartOffset;
    private long currentTrackingOffset;

    /**
     * Creates a new gap tracker.
     *
     * @param endOffset      The end offset of the gap window (fixed).
     * @param gapStartOffset The initial start offset where gaps may begin.
     */
    public PersisterGapTracker(long endOffset, long gapStartOffset) {
        this.endOffset = endOffset;
        this.gapStartOffset = gapStartOffset;
        this.currentTrackingOffset = -1;
    }

    /**
     * Checks if this gap tracker is still active based on current end offset.
     * The gap window becomes inactive when the share partition's endOffset
     * moves past the gap window's endOffset.
     *
     * @param currentEndOffset The current end offset of the share partition.
     * @return True if gap tracking is still relevant.
     */
    public boolean isActive(long currentEndOffset) {
        return this.endOffset == currentEndOffset;
    }

    /**
     * Initializes tracking for a new acquisition loop.
     * Must be called at the start of each acquire iteration.
     *
     * @param baseOffset The base offset to start tracking from.
     */
    public void startTracking(long baseOffset) {
        this.currentTrackingOffset = baseOffset;
    }

    /**
     * Checks if there's a gap before the given batch that needs to be acquired.
     * A gap exists when the current tracking offset is less than the next
     * batch's first offset, meaning there are offsets in between that aren't
     * in the cached state.
     *
     * @param batchFirstOffset The first offset of the next cached batch.
     * @return An optional GapRange if a gap exists, empty otherwise.
     */
    public Optional<GapRange> findGapBefore(long batchFirstOffset) {
        // If current tracking gap base offset is less than the cached state batch, this means the fetch happened for a
        // gap in the cachedState. Thus, a new batch needs to be acquired for the gap.
        if (currentTrackingOffset >= 0 && currentTrackingOffset < batchFirstOffset) {
            // It's safe to use batchFirstOffset - 1 as the last offset to acquire for the
            // gap as the sub map should contain either the next batch or this line should
            // not have been executed i.e. say there is a gap from 10-20 and cache contains
            // [0-9, 21-30], when fetch returns single/multiple batches from 0-15, then
            // first sub map entry has no gap and there exists only 1 entry in sub map.
            // Hence, for next batch the following code will not be executed and records
            // from 10-15 will be acquired later in the code. In other case, when
            // fetch returns batches from 0-25, then the sub map will have 2 entries and
            // gap will be computed correctly.
            return Optional.of(new GapRange(currentTrackingOffset, batchFirstOffset - 1));
        }
        return Optional.empty();
    }

    /**
     * Advances the tracking offset past the given batch.
     * Should be called after processing each cached batch to track
     * where the next potential gap might start.
     *
     * @param batchLastOffset The last offset of the batch just processed.
     */
    public void advancePast(long batchLastOffset) {
        this.currentTrackingOffset = batchLastOffset + 1;
    }

    /**
     * Updates the gap start offset after successful acquisition.
     * <p>
     * The gap window remains active only if:
     * <ul>
     *   <li>The share partition's endOffset hasn't changed</li>
     *   <li>The new offset is within the gap window boundary</li>
     * </ul>
     *
     * @param newOffset        The new offset to set as gap start.
     * @param currentEndOffset The current end offset of the share partition.
     * @return True if the gap window is still active, false if it should be cleared.
     */
    public boolean maybeUpdateGapStart(long newOffset, long currentEndOffset) {
        if (this.endOffset == currentEndOffset && newOffset <= this.endOffset) {
            this.gapStartOffset = newOffset;
            return true;
        }
        return false;
    }

    /**
     * @return The current gap start offset.
     */
    public long gapStartOffset() {
        return gapStartOffset;
    }

    /**
     * @return The fixed end offset of the gap window.
     */
    public long endOffset() {
        return endOffset;
    }

    /**
     * Represents a gap range that needs to be acquired.
     *
     * @param firstOffset The first offset of the gap (inclusive).
     * @param lastOffset  The last offset of the gap (inclusive).
     */
    public record GapRange(long firstOffset, long lastOffset) {
    }
}