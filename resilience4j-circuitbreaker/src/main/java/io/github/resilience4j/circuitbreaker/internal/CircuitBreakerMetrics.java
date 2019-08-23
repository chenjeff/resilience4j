/*
 *
 *  Copyright 2016 Robert Winkler and Bohdan Storozhuk
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package io.github.resilience4j.circuitbreaker.internal;


import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.core.lang.Nullable;

import java.util.concurrent.atomic.LongAdder;

class CircuitBreakerMetrics implements CircuitBreaker.Metrics {

    private final int ringBufferSize;
    /**
     * 失败置1
     */
    private final RingBitSet ringBitSet;

    /**
     * 拒绝数
     */
    private final LongAdder numberOfNotPermittedCalls;

    CircuitBreakerMetrics(int ringBufferSize) {
        this(ringBufferSize, null);
    }

    CircuitBreakerMetrics(int ringBufferSize, @Nullable RingBitSet sourceSet) {
        this.ringBufferSize = ringBufferSize;
        if (sourceSet != null) {
            this.ringBitSet = new RingBitSet(this.ringBufferSize, sourceSet);
        } else {
            this.ringBitSet = new RingBitSet(this.ringBufferSize);
        }

        this.numberOfNotPermittedCalls = new LongAdder();
    }

    /**
     * Creates a new CircuitBreakerMetrics instance and copies the content of the current RingBitSet
     * into the new RingBitSet.
     *
     * @param targetRingBufferSize the ringBufferSize of the new CircuitBreakerMetrics instances
     * @return a CircuitBreakerMetrics
     */
    public CircuitBreakerMetrics copy(int targetRingBufferSize) {
        return new CircuitBreakerMetrics(targetRingBufferSize, this.ringBitSet);
    }

    /**
     * Records a failed call and returns the current failure rate in percentage.
     *
     * @return the current failure rate  in percentage.
     */
    float onError() {
        // 失败置1
        int currentNumberOfFailedCalls = ringBitSet.setNextBit(true);

        // 计算并返回失败率
        return getFailureRate(currentNumberOfFailedCalls);
    }

    /**
     * Records a successful call and returns the current failure rate in percentage.
     *
     * @return the current failure rate in percentage.
     */
    float onSuccess() {
        // 成功置0
        int currentNumberOfFailedCalls = ringBitSet.setNextBit(false);

        // 计算并返回失败率
        return getFailureRate(currentNumberOfFailedCalls);
    }

    /**
     * Records a call which was not permitted, because the CircuitBreaker state is OPEN.
     */
    void onCallNotPermitted() {
        // 拒绝数 +1
        numberOfNotPermittedCalls.increment();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFailureRate() {
        // 计算并返回失败率
        return getFailureRate(getNumberOfFailedCalls());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxNumberOfBufferedCalls() {
        // 缓存总数量
        return ringBufferSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfSuccessfulCalls() {
        // 成功数 = 已缓存总数 - 失败数
        return getNumberOfBufferedCalls() - getNumberOfFailedCalls();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfBufferedCalls() {
        // 已缓存的数量
        return this.ringBitSet.length();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNumberOfNotPermittedCalls() {
        // 拒绝总数
        return this.numberOfNotPermittedCalls.sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfFailedCalls() {
        // 失败数量
        return this.ringBitSet.cardinality();
    }

    /**
     * 计算失败率
     *
     * @param numberOfFailedCalls
     * @return
     */
    private float getFailureRate(int numberOfFailedCalls) {
        // 未填满不计算
        if (getNumberOfBufferedCalls() < ringBufferSize) {
            return -1.0f;
        }

        return numberOfFailedCalls * 100.0f / ringBufferSize;
    }

}
