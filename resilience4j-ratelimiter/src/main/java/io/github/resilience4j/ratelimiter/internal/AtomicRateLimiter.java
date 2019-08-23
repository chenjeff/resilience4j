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
package io.github.resilience4j.ratelimiter.internal;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.event.RateLimiterOnFailureEvent;
import io.github.resilience4j.ratelimiter.event.RateLimiterOnSuccessEvent;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static java.lang.Long.min;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * {@link AtomicRateLimiter} splits all nanoseconds from the start of epoch into cycles.
 * <p>Each cycle has duration of {@link RateLimiterConfig#limitRefreshPeriod} in nanoseconds.
 * <p>By contract on start of each cycle {@link AtomicRateLimiter} should
 * set {@link State#activePermissions} to {@link RateLimiterConfig#limitForPeriod}.
 * For the {@link AtomicRateLimiter} callers it is really looks so, but under the hood there is
 * some optimisations that will skip this refresh if {@link AtomicRateLimiter} is not used actively.
 * <p>All {@link AtomicRateLimiter} updates are atomic and state is encapsulated in {@link AtomicReference} to
 * {@link AtomicRateLimiter.State}
 */
public class AtomicRateLimiter implements RateLimiter {

    private static final long nanoTimeStart = nanoTime();

    private final String name;
    private final AtomicInteger waitingThreads;
    private final AtomicReference<State> state;
    private final RateLimiterEventProcessor eventProcessor;

    public AtomicRateLimiter(String name, RateLimiterConfig rateLimiterConfig) {
        this.name = name;

        waitingThreads = new AtomicInteger(0);
        state = new AtomicReference<>(new State(rateLimiterConfig, 0, rateLimiterConfig.getLimitForPeriod(), 0));
        eventProcessor = new RateLimiterEventProcessor();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void changeTimeoutDuration(final Duration timeoutDuration) {
        RateLimiterConfig newConfig = RateLimiterConfig.from(state.get().config)
                .timeoutDuration(timeoutDuration)
                .build();

        state.updateAndGet(currentState -> new State(newConfig, currentState.activeCycle, currentState.activePermissions, currentState.nanosToWait));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void changeLimitForPeriod(final int limitForPeriod) {
        RateLimiterConfig newConfig = RateLimiterConfig.from(state.get().config)
                .limitForPeriod(limitForPeriod)
                .build();

        state.updateAndGet(currentState -> new State(newConfig, currentState.activeCycle, currentState.activePermissions, currentState.nanosToWait));
    }

    /**
     * Calculates time elapsed from the class loading.
     */
    private long currentNanoTime() {
        return nanoTime() - nanoTimeStart;
    }

    /**
     * 阻塞获取token
     *
     * @return true: 成功; false: 失败;
     */
    @Override
    public boolean acquirePermission() {
        // 超时时间
        long timeoutInNanos = state.get().config.getTimeoutDuration().toNanos();
        State modifiedState = updateStateWithBackOff(timeoutInNanos);

        boolean result = waitForPermissionIfNecessary(timeoutInNanos, modifiedState.nanosToWait);
        // 触发事件
        publishRateLimiterEvent(result);

        return result;
    }

    /**
     * 非阻塞获取token, 程序自行控制
     *
     * @return 0: 获取成功; -1: 获取失败; 大于0: 需等待时长
     */
    @Override
    public long reservePermission() {
        long timeoutInNanos = state.get().config.getTimeoutDuration().toNanos();
        State modifiedState = updateStateWithBackOff(timeoutInNanos);

        // 是否需要等待token产生; 当前是否存在可用token
        boolean canAcquireImmediately = modifiedState.nanosToWait <= 0;
        if (canAcquireImmediately) {
            // 有可用token, 无需等待
            publishRateLimiterEvent(true);
            return 0;
        }

        // 是否会超时
        boolean canAcquireInTime = timeoutInNanos >= modifiedState.nanosToWait;
        if (canAcquireInTime) {
            // 不会超时
            publishRateLimiterEvent(true);
            // 返回: 等待时间, 程序自行处理
            return modifiedState.nanosToWait;
        }

        // 超时
        publishRateLimiterEvent(false);

        return -1;
    }

    /**
     * Atomically updates the current {@link State} with the results of
     * applying the {@link AtomicRateLimiter#calculateNextState}, returning the updated {@link State}.
     * It differs from {@link AtomicReference#updateAndGet(UnaryOperator)} by constant back off.
     * It means that after one try to {@link AtomicReference#compareAndSet(Object, Object)}
     * this method will wait for a while before try one more time.
     * This technique was originally described in this
     * <a href="https://arxiv.org/abs/1305.5800"> paper</a>
     * and showed great results with {@link AtomicRateLimiter} in benchmark tests.
     *
     * @param timeoutInNanos a side-effect-free function
     * @return the updated value
     */
    private State updateStateWithBackOff(final long timeoutInNanos) {
        AtomicRateLimiter.State prev;
        AtomicRateLimiter.State next;
        do {
            prev = state.get();
            next = calculateNextState(timeoutInNanos, prev);
            // 通过 CAS 保证顺序
        } while (!compareAndSet(prev, next));

        return next;
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     * It differs from {@link AtomicReference#updateAndGet(UnaryOperator)} by constant back off.
     * It means that after one try to {@link AtomicReference#compareAndSet(Object, Object)}
     * this method will wait for a while before try one more time.
     * This technique was originally described in this
     * <a href="https://arxiv.org/abs/1305.5800"> paper</a>
     * and showed great results with {@link AtomicRateLimiter} in benchmark tests.
     *
     * @param current the expected value
     * @param next    the new value
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    private boolean compareAndSet(final State current, final State next) {
        if (state.compareAndSet(current, next)) {
            return true;
        }

        // back-off
        parkNanos(1);
        return false;
    }

    /**
     * A side-effect-free function that can calculate next {@link State} from current.
     * It determines time duration that you should wait for permission and reserves it for you,
     * if you'll be able to wait long enough.
     *
     * @param timeoutInNanos max time that caller can wait for permission in nanoseconds
     * @param activeState    current state of {@link AtomicRateLimiter}
     * @return next {@link State}
     */
    private State calculateNextState(final long timeoutInNanos, final State activeState) {
        // 刷新周期: default: 500纳秒
        long cyclePeriodInNanos = activeState.config.getLimitRefreshPeriod().toNanos();
        // 周期内允许的访token量: default: 50个
        int permissionsPerCycle = activeState.config.getLimitForPeriod();

        // 限流器启动到现在(当前时间)经过的时间 单位: 纳秒
        long currentNanos = currentNanoTime();
        // 当前属于第几个周期
        long currentCycle = currentNanos / cyclePeriodInNanos;

        // Stage当前周期
        long nextCycle = activeState.activeCycle;
        // Stage当前token数量(可用token数量) 可能为: 负数
        int nextPermissions = activeState.activePermissions;

        // 判断下一个周期跟当前周期是否相同，不同则计算周期允许的token数量
        if (nextCycle != currentCycle) {
            // 一直不被触发可能已经过多个周期; 存在跨多个周期这种情况;
            // currentCycle 大于 nextCycle --> elapsedCycles 大于 零
            long elapsedCycles = currentCycle - nextCycle;
            // token积累数量
            long accumulatedPermissions = elapsedCycles * permissionsPerCycle;
            // 更新周期
            nextCycle = currentCycle;
            // 避免产生大于周期配置的允许token数量: 取较小值
            nextPermissions = (int) min(nextPermissions + accumulatedPermissions, permissionsPerCycle);
        }

        // 计算下一周期token等待时间（计算轮到当前线程获取token，需等待的时间）
        long nextNanosToWait = nanosToWaitForPermission(cyclePeriodInNanos, permissionsPerCycle, nextPermissions, currentNanos, currentCycle);

        State nextState = reservePermissions(activeState.config, timeoutInNanos, nextCycle, nextPermissions, nextNanosToWait);

        return nextState;
    }

    /**
     * Calculates time to wait for next permission as
     * [time to the next cycle] + [duration of full cycles until reserved permissions expire]
     *
     * @param cyclePeriodInNanos   current configuration values
     * @param permissionsPerCycle  current configuration values
     * @param availablePermissions currently available permissions, can be negative if some permissions have been reserved
     * @param currentNanos         current time in nanoseconds
     * @param currentCycle         current {@link AtomicRateLimiter} cycle    @return nanoseconds to wait for the next permission
     */
    private long nanosToWaitForPermission(final long cyclePeriodInNanos /*  刷新周期: default: 500纳秒 */,
                                          final int permissionsPerCycle /*  周期内允许的访token量: default: 50个 */,
                                          final int availablePermissions /*  当前可用token数量 */,
                                          final long currentNanos /*  限流器启动到现在(当前时间)经过的时间 单位: 纳秒 */,
                                          final long currentCycle /*  当前属于第几个周期 */) {
        // 可用token大于0, 直接放行。无需等待
        if (availablePermissions > 0) {
            return 0L;
        }

        // 下一周期，纳秒数
        long nextCycleTimeInNanos = (currentCycle + 1) * cyclePeriodInNanos;

        // 到达下一周期，需等待的时间
        long nanosToNextCycle = nextCycleTimeInNanos - currentNanos;

        // 产生全部token，所需要的周期数
        int fullCyclesToWait = (-availablePermissions) / permissionsPerCycle;

        // 周期数 * 周期时间 + 当前到达下一周期的时间
        return (fullCyclesToWait * cyclePeriodInNanos) + nanosToNextCycle;
    }

    /**
     * Determines whether caller can acquire permission before timeout or not and then creates corresponding {@link State}.
     * Reserves permissions only if caller can successfully wait for permission.
     *
     * @param config
     * @param timeoutInNanos max time that caller can wait for permission in nanoseconds
     * @param cycle          cycle for new {@link State}
     * @param permissions    permissions for new {@link State}
     * @param nanosToWait    nanoseconds to wait for the next permission
     * @return new {@link State} with possibly reserved permissions and time to wait
     */
    private State reservePermissions(final RateLimiterConfig config /*  配置文件 */,
                                     final long timeoutInNanos /*  超时时间 */,
                                     final long cycle /*  当前属于第几个周期 */,
                                     final int permissions /*  当前可用token数量 */,
                                     final long nanosToWait /*  当前线程需等待的时间 */) {

        // 等待时间是否超时
        boolean canAcquireInTime = timeoutInNanos >= nanosToWait;
        int permissionsWithReservation = permissions;
        if (canAcquireInTime) {
            // 未超时
            permissionsWithReservation--;
        }

        return new State(config, cycle, permissionsWithReservation, nanosToWait);
    }

    /**
     * If nanosToWait is bigger than 0 it tries to park {@link Thread} for nanosToWait but not longer then timeoutInNanos.
     *
     * @param timeoutInNanos max time that caller can wait
     * @param nanosToWait    nanoseconds caller need to wait
     * @return true if caller was able to wait for nanosToWait without {@link Thread#interrupt} and not exceed timeout
     */
    private boolean waitForPermissionIfNecessary(final long timeoutInNanos, final long nanosToWait) {
        // 当前线程是否需要等待
        boolean canAcquireImmediately = nanosToWait <= 0;
        // 等待时间是否超时
        boolean canAcquireInTime = timeoutInNanos >= nanosToWait;

        if (canAcquireImmediately) {
            // 无需等待
            return true;
        }

        if (canAcquireInTime) {
            // 未超时
            return waitForPermission(nanosToWait);
        }

        // 超时
        waitForPermission(timeoutInNanos);
        return false;
    }

    /**
     * Parks {@link Thread} for nanosToWait.
     * <p>If the current thread is {@linkplain Thread#interrupted}
     * while waiting for a permit then it won't throw {@linkplain InterruptedException},
     * but its interrupt status will be set.
     *
     * @param nanosToWait nanoseconds caller need to wait
     * @return true if caller was not {@link Thread#interrupted} while waiting
     */
    private boolean waitForPermission(final long nanosToWait) {
        // 等待计数器 +1
        waitingThreads.incrementAndGet();
        // 等待时间终点
        long deadline = currentNanoTime() + nanosToWait;
        boolean wasInterrupted = false;
        // 判断是否需要堵塞
        while (currentNanoTime() < deadline && !wasInterrupted) {
            // 计算堵塞时长
            long sleepBlockDuration = deadline - currentNanoTime();
            // 线程等待
            parkNanos(sleepBlockDuration);
            wasInterrupted = Thread.interrupted();
        }

        // 计数器复位
        waitingThreads.decrementAndGet();
        if (wasInterrupted) {
            currentThread().interrupt();
        }

        return !wasInterrupted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RateLimiterConfig getRateLimiterConfig() {
        return state.get().config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Metrics getMetrics() {
        return new AtomicRateLimiterMetrics();
    }

    @Override
    public EventPublisher getEventPublisher() {
        return eventProcessor;
    }

    @Override
    public String toString() {
        return "AtomicRateLimiter{" +
                "name='" + name + '\'' +
                ", rateLimiterConfig=" + state.get().config +
                '}';
    }

    /**
     * Get the enhanced Metrics with some implementation specific details.
     *
     * @return the detailed metrics
     */
    public AtomicRateLimiterMetrics getDetailedMetrics() {
        return new AtomicRateLimiterMetrics();
    }

    private void publishRateLimiterEvent(boolean permissionAcquired) {
        if (!eventProcessor.hasConsumers()) {
            return;
        }

        if (permissionAcquired) {
            eventProcessor.consumeEvent(new RateLimiterOnSuccessEvent(name));
            return;
        }

        eventProcessor.consumeEvent(new RateLimiterOnFailureEvent(name));
    }

    /**
     * <p>{@link AtomicRateLimiter.State} represents immutable state of {@link AtomicRateLimiter} where:
     * <ul>
     * <li>activeCycle - {@link AtomicRateLimiter} cycle number that was used
     * by the last {@link AtomicRateLimiter#acquirePermission()} call.</li>
     * <p>
     * <li>activePermissions - count of available permissions after
     * the last {@link AtomicRateLimiter#acquirePermission()} call.
     * Can be negative if some permissions where reserved.</li>
     * <p>
     * <li>nanosToWait - count of nanoseconds to wait for permission for
     * the last {@link AtomicRateLimiter#acquirePermission()} call.</li>
     * </ul>
     */
    private static class State {

        private final RateLimiterConfig config;

        private final long activeCycle;
        private final int activePermissions;
        private final long nanosToWait;

        private State(RateLimiterConfig config, final long activeCycle, final int activePermissions, final long nanosToWait) {
            this.config = config;
            this.activeCycle = activeCycle;
            this.activePermissions = activePermissions;
            this.nanosToWait = nanosToWait;
        }

    }

    /**
     * Enhanced {@link Metrics} with some implementation specific details
     */
    public class AtomicRateLimiterMetrics implements Metrics {

        private AtomicRateLimiterMetrics() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getNumberOfWaitingThreads() {
            return waitingThreads.get();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getAvailablePermissions() {
            State currentState = state.get();
            State estimatedState = calculateNextState(-1, currentState);
            return estimatedState.activePermissions;
        }

        /**
         * @return estimated time duration in nanos to wait for the next permission
         */
        public long getNanosToWait() {
            State currentState = state.get();
            State estimatedState = calculateNextState(-1, currentState);
            return estimatedState.nanosToWait;
        }

        /**
         * @return estimated current cycle
         */
        public long getCycle() {
            State currentState = state.get();
            State estimatedState = calculateNextState(-1, currentState);
            return estimatedState.activeCycle;
        }

    }

}
