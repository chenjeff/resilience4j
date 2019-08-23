/*
 *
 *  Copyright 2019 Robert Winkler
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

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.event.*;
import io.github.resilience4j.core.EventConsumer;
import io.github.resilience4j.core.EventProcessor;
import io.github.resilience4j.core.lang.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.*;

/**
 * A CircuitBreaker finite state machine.
 */
public final class CircuitBreakerStateMachine implements CircuitBreaker {

    private static final Logger LOG = LoggerFactory.getLogger(CircuitBreakerStateMachine.class);

    private final String name;
    private final AtomicReference<CircuitBreakerState> stateReference;
    private final CircuitBreakerConfig circuitBreakerConfig;
    private final CircuitBreakerEventProcessor eventProcessor;
    private final Clock clock;
    private final SchedulerFactory schedulerFactory;

    /**
     * Creates a circuitBreaker.
     *
     * @param name                 the name of the CircuitBreaker
     * @param circuitBreakerConfig The CircuitBreaker configuration.
     * @param clock                A Clock which can be mocked in tests.
     * @param schedulerFactory     A SchedulerFactory which can be mocked in tests.
     */
    private CircuitBreakerStateMachine(String name,
                                       CircuitBreakerConfig circuitBreakerConfig,
                                       Clock clock,
                                       SchedulerFactory schedulerFactory) {
        this.name = name;
        this.circuitBreakerConfig = Objects.requireNonNull(circuitBreakerConfig, "Config must not be null");
        this.stateReference = new AtomicReference<>(new ClosedState());
        this.eventProcessor = new CircuitBreakerEventProcessor();
        this.clock = clock;
        this.schedulerFactory = schedulerFactory;
    }

    /**
     * Creates a circuitBreaker.
     *
     * @param name                 the name of the CircuitBreaker
     * @param circuitBreakerConfig The CircuitBreaker configuration.
     * @param schedulerFactory     A SchedulerFactory which can be mocked in tests.
     */
    public CircuitBreakerStateMachine(String name,
                                      CircuitBreakerConfig circuitBreakerConfig,
                                      SchedulerFactory schedulerFactory) {
        this(name, circuitBreakerConfig, Clock.systemUTC(), schedulerFactory);
    }

    /**
     * Creates a circuitBreaker.
     *
     * @param name                 the name of the CircuitBreaker
     * @param circuitBreakerConfig The CircuitBreaker configuration.
     */
    public CircuitBreakerStateMachine(String name,
                                      CircuitBreakerConfig circuitBreakerConfig,
                                      Clock clock) {
        this(name, circuitBreakerConfig, clock, SchedulerFactory.getInstance());
    }

    /**
     * Creates a circuitBreaker.
     *
     * @param name                 the name of the CircuitBreaker
     * @param circuitBreakerConfig The CircuitBreaker configuration.
     */
    public CircuitBreakerStateMachine(String name,
                                      CircuitBreakerConfig circuitBreakerConfig) {
        this(name, circuitBreakerConfig, Clock.systemUTC());
    }

    /**
     * Creates a circuitBreaker with default config.
     *
     * @param name the name of the CircuitBreaker
     */
    public CircuitBreakerStateMachine(String name) {
        this(name, CircuitBreakerConfig.ofDefaults());
    }

    /**
     * Creates a circuitBreaker.
     *
     * @param name                 the name of the CircuitBreaker
     * @param circuitBreakerConfig The CircuitBreaker configuration supplier.
     */
    public CircuitBreakerStateMachine(String name, Supplier<CircuitBreakerConfig> circuitBreakerConfig) {
        this(name, circuitBreakerConfig.get());
    }

    @Override
    public boolean tryAcquirePermission() {
        boolean callPermitted = stateReference.get().tryAcquirePermission();
        if (!callPermitted) {
            publishCallNotPermittedEvent();
        }

        return callPermitted;
    }

    @Override
    public void releasePermission() {
        stateReference.get().releasePermission();
    }

    @Override
    public void acquirePermission() {
        try {
            stateReference.get().acquirePermission();
        } catch (Exception e) {
            publishCallNotPermittedEvent();
            throw e;
        }
    }

    @Override
    public void onError(long durationInNanos, Throwable throwable) {
        // Handle the case if the completable future throw CompletionException wrapping the original exception
        // where original exception is the the one to retry not the CompletionException.
        Predicate<Throwable> recordFailurePredicate = circuitBreakerConfig.getRecordFailurePredicate();
        if (throwable instanceof CompletionException) {
            Throwable cause = throwable.getCause();
            handleThrowable(durationInNanos, recordFailurePredicate, cause);
        } else {
            handleThrowable(durationInNanos, recordFailurePredicate, throwable);
        }
    }

    private void handleThrowable(long durationInNanos, Predicate<Throwable> recordFailurePredicate, Throwable throwable) {
        if (recordFailurePredicate.test(throwable)) {
            LOG.debug("CircuitBreaker '{}' recorded a failure:", name, throwable);
            publishCircuitErrorEvent(name, durationInNanos, throwable);
            stateReference.get().onError(throwable);
        } else {
            releasePermission();
            publishCircuitIgnoredErrorEvent(name, durationInNanos, throwable);
        }
    }

    @Override
    public void onSuccess(long durationInNanos) {
        publishSuccessEvent(durationInNanos);
        stateReference.get().onSuccess();
    }

    /**
     * Get the state of this CircuitBreaker.
     *
     * @return the the state of this CircuitBreaker
     */
    @Override
    public State getState() {
        return this.stateReference.get().getState();
    }

    /**
     * Get the name of this CircuitBreaker.
     *
     * @return the the name of this CircuitBreaker
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * Get the config of this CircuitBreaker.
     *
     * @return the config of this CircuitBreaker
     */
    @Override
    public CircuitBreakerConfig getCircuitBreakerConfig() {
        return circuitBreakerConfig;
    }

    @Override
    public Metrics getMetrics() {
        return this.stateReference.get().getMetrics();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("CircuitBreaker '%s'", this.name);
    }

    /**
     * 重置断路器
     */
    @Override
    public void reset() {
        CircuitBreakerState previousState = stateReference.getAndUpdate(currentState -> new ClosedState());
        if (previousState.getState() != CLOSED) {
            publishStateTransitionEvent(StateTransition.transitionBetween(previousState.getState(), CLOSED));
        }

        publishResetEvent();
    }

    /**
     * 改变断路器状态
     *
     * @param newState
     * @param newStateGenerator
     */
    private void stateTransition(State newState, UnaryOperator<CircuitBreakerState> newStateGenerator) {
        CircuitBreakerState previousState = stateReference.getAndUpdate(currentState -> {
            if (currentState.getState() == newState) {
                return currentState;
            }

            // apply 相当于传参 --> 需要数据监控器 作为入参
            return newStateGenerator.apply(currentState);
        });

        if (previousState.getState() != newState) {
            // 发布状态改变事件
            publishStateTransitionEvent(StateTransition.transitionBetween(previousState.getState(), newState));
        }
    }

    /**
     * 失效断路器，允许所有请求通过
     */
    @Override
    public void transitionToDisabledState() {
        stateTransition(DISABLED, currentState -> new DisabledState());
    }

    /**
     * 强制打开断路器
     */
    @Override
    public void transitionToForcedOpenState() {
        stateTransition(FORCED_OPEN, currentState -> new ForcedOpenState());
    }

    /**
     * 切换断路器状态至: 关闭状态
     */
    @Override
    public void transitionToClosedState() {
        stateTransition(CLOSED, currentState -> new ClosedState(currentState.getMetrics()));
    }

    /**
     * 切换断路器状态至: 打开状态
     */
    @Override
    public void transitionToOpenState() {
        stateTransition(OPEN, currentState -> new OpenState(currentState.getMetrics()));
    }

    /**
     * 切换断路器状态至: 半开状态
     */
    @Override
    public void transitionToHalfOpenState() {
        stateTransition(HALF_OPEN, currentState -> new HalfOpenState());
    }

    /**
     * 是否允许发布事件
     *
     * @param event
     * @return
     */
    private boolean shouldPublishEvents(CircuitBreakerEvent event) {
        // 与相关{State, Type}
        return stateReference.get().shouldPublishEvents(event);
    }

    /**
     * 需要则发布事件
     *
     * @param event
     */
    private void publishEventIfPossible(CircuitBreakerEvent event) {
        if (shouldPublishEvents(event)) {
            if (eventProcessor.hasConsumers()) {
                LOG.debug("Event {} published: {}", event.getEventType(), event);
                try {
                    eventProcessor.consumeEvent(event);
                } catch (Throwable t) {
                    LOG.warn("Failed to handle event {}", event.getEventType(), t);
                }
            } else {
                LOG.debug("No Consumers: Event {} not published", event.getEventType());
            }
        } else {
            LOG.debug("Publishing not allowed: Event {} not published", event.getEventType());
        }
    }

    private void publishStateTransitionEvent(final StateTransition stateTransition) {
        final CircuitBreakerOnStateTransitionEvent event = new CircuitBreakerOnStateTransitionEvent(name, stateTransition);
        publishEventIfPossible(event);
    }

    private void publishResetEvent() {
        final CircuitBreakerOnResetEvent event = new CircuitBreakerOnResetEvent(name);
        publishEventIfPossible(event);
    }

    private void publishCallNotPermittedEvent() {
        final CircuitBreakerOnCallNotPermittedEvent event = new CircuitBreakerOnCallNotPermittedEvent(name);
        publishEventIfPossible(event);
    }

    private void publishSuccessEvent(final long durationInNanos) {
        final CircuitBreakerOnSuccessEvent event = new CircuitBreakerOnSuccessEvent(name, Duration.ofNanos(durationInNanos));
        publishEventIfPossible(event);
    }

    private void publishCircuitErrorEvent(final String name, final long durationInNanos, final Throwable throwable) {
        final CircuitBreakerOnErrorEvent event = new CircuitBreakerOnErrorEvent(name, Duration.ofNanos(durationInNanos), throwable);
        publishEventIfPossible(event);
    }

    private void publishCircuitIgnoredErrorEvent(String name, long durationInNanos, Throwable throwable) {
        final CircuitBreakerOnIgnoredErrorEvent event = new CircuitBreakerOnIgnoredErrorEvent(name, Duration.ofNanos(durationInNanos), throwable);
        publishEventIfPossible(event);
    }

    @Override
    public EventPublisher getEventPublisher() {
        return eventProcessor;
    }

    private class CircuitBreakerEventProcessor extends EventProcessor<CircuitBreakerEvent> implements EventConsumer<CircuitBreakerEvent>, EventPublisher {

        @Override
        public EventPublisher onSuccess(EventConsumer<CircuitBreakerOnSuccessEvent> onSuccessEventConsumer) {
            registerConsumer(CircuitBreakerOnSuccessEvent.class.getSimpleName(), onSuccessEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onError(EventConsumer<CircuitBreakerOnErrorEvent> onErrorEventConsumer) {
            registerConsumer(CircuitBreakerOnErrorEvent.class.getSimpleName(), onErrorEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onStateTransition(EventConsumer<CircuitBreakerOnStateTransitionEvent> onStateTransitionEventConsumer) {
            registerConsumer(CircuitBreakerOnStateTransitionEvent.class.getSimpleName(), onStateTransitionEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onReset(EventConsumer<CircuitBreakerOnResetEvent> onResetEventConsumer) {
            registerConsumer(CircuitBreakerOnResetEvent.class.getSimpleName(), onResetEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onIgnoredError(EventConsumer<CircuitBreakerOnIgnoredErrorEvent> onIgnoredErrorEventConsumer) {
            registerConsumer(CircuitBreakerOnIgnoredErrorEvent.class.getSimpleName(), onIgnoredErrorEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onCallNotPermitted(EventConsumer<CircuitBreakerOnCallNotPermittedEvent> onCallNotPermittedEventConsumer) {
            registerConsumer(CircuitBreakerOnCallNotPermittedEvent.class.getSimpleName(), onCallNotPermittedEventConsumer);
            return this;
        }

        @Override
        public void consumeEvent(CircuitBreakerEvent event) {
            super.processEvent(event);
        }
    }

    private class ClosedState implements CircuitBreakerState {

        private final CircuitBreakerMetrics circuitBreakerMetrics;
        private final float failureRateThreshold;

        ClosedState() {
            this(null);
        }

        ClosedState(@Nullable CircuitBreakerMetrics circuitBreakerMetrics) {
            if (circuitBreakerMetrics == null) {
                this.circuitBreakerMetrics = new CircuitBreakerMetrics(circuitBreakerConfig.getRingBufferSizeInClosedState());
            } else {
                this.circuitBreakerMetrics = circuitBreakerMetrics.copy(circuitBreakerConfig.getRingBufferSizeInClosedState());
            }

            this.failureRateThreshold = circuitBreakerConfig.getFailureRateThreshold();
        }

        /**
         * Returns always true, because the CircuitBreaker is closed.
         *
         * @return always true, because the CircuitBreaker is closed.
         */
        @Override
        public boolean tryAcquirePermission() {
            return true;
        }

        /**
         * Does not throw an exception, because the CircuitBreaker is closed.
         */
        @Override
        public void acquirePermission() {
            // noOp
        }

        @Override
        public void releasePermission() {
            // noOp
        }

        @Override
        public void onError(Throwable throwable) {
            // CircuitBreakerMetrics is thread-safe
            checkFailureRate(circuitBreakerMetrics.onError());
        }

        @Override
        public void onSuccess() {
            // CircuitBreakerMetrics is thread-safe
            checkFailureRate(circuitBreakerMetrics.onSuccess());
        }

        /**
         * Checks if the current failure rate is above the threshold.
         * If the failure rate is above the threshold, transitions the state machine to OPEN state.
         *
         * @param currentFailureRate the current failure rate
         */
        private void checkFailureRate(float currentFailureRate) {
            if (currentFailureRate >= failureRateThreshold) {
                // Transition the state machine to OPEN state, because the failure rate is above the threshold
                transitionToOpenState();
            }
        }

        /**
         * Get the state of the CircuitBreaker
         */
        @Override
        public CircuitBreaker.State getState() {
            return CircuitBreaker.State.CLOSED;
        }

        /**
         * Get metrics of the CircuitBreaker
         */
        @Override
        public CircuitBreakerMetrics getMetrics() {
            return circuitBreakerMetrics;
        }

    }

    private class OpenState implements CircuitBreakerState {

        private final Instant retryAfterWaitDuration;
        private final CircuitBreakerMetrics circuitBreakerMetrics;

        OpenState(CircuitBreakerMetrics circuitBreakerMetrics) {
            // 断路器打开持续时间 default: 60秒
            final Duration waitDurationInOpenState = circuitBreakerConfig.getWaitDurationInOpenState();
            // 重试时间
            this.retryAfterWaitDuration = clock.instant().plus(waitDurationInOpenState);
            // 数据监控器
            this.circuitBreakerMetrics = circuitBreakerMetrics;

            // 是否自动断路器状态，从打开状态切换至半开状态
            if (circuitBreakerConfig.isAutomaticTransitionFromOpenToHalfOpenEnabled()) {
                ScheduledExecutorService scheduledExecutorService = schedulerFactory.getScheduler();
                scheduledExecutorService.schedule(CircuitBreakerStateMachine.this::transitionToHalfOpenState, waitDurationInOpenState.toMillis(), TimeUnit.MILLISECONDS);
            }
        }

        /**
         * Returns false, if the wait duration has not elapsed.
         * Returns true, if the wait duration has elapsed and transitions the state machine to HALF_OPEN state.
         *
         * @return false, if the wait duration has not elapsed. true, if the wait duration has elapsed.
         */
        @Override
        public boolean tryAcquirePermission() {
            // Thread-safe
            if (clock.instant().isAfter(retryAfterWaitDuration)) {
                // 超时
                transitionToHalfOpenState();
                return true;
            }

            circuitBreakerMetrics.onCallNotPermitted();
            return false;
        }

        @Override
        public void acquirePermission() {
            // OpenState
            if (!tryAcquirePermission()) {
                throw new CallNotPermittedException(CircuitBreakerStateMachine.this);
            }
        }

        @Override
        public void releasePermission() {
            // noOp
        }

        /**
         * Should never be called when tryAcquirePermission returns false.
         */
        @Override
        public void onError(Throwable throwable) {
            // Could be called when Thread 1 invokes acquirePermission when the state is CLOSED, but in the meantime another
            // Thread 2 calls onError and the state changes from CLOSED to OPEN before Thread 1 calls onError.
            // But the onError event should still be recorded, even if it happened after the state transition.
            circuitBreakerMetrics.onError();
        }

        /**
         * Should never be called when tryAcquirePermission returns false.
         */
        @Override
        public void onSuccess() {
            // Could be called when Thread 1 invokes acquirePermission when the state is CLOSED, but in the meantime another
            // Thread 2 calls onError and the state changes from CLOSED to OPEN before Thread 1 calls onSuccess.
            // But the onSuccess event should still be recorded, even if it happened after the state transition.
            circuitBreakerMetrics.onSuccess();
        }

        /**
         * Get the state of the CircuitBreaker
         */
        @Override
        public CircuitBreaker.State getState() {
            return CircuitBreaker.State.OPEN;
        }

        @Override
        public CircuitBreakerMetrics getMetrics() {
            return circuitBreakerMetrics;
        }
    }

    private class DisabledState implements CircuitBreakerState {

        private final CircuitBreakerMetrics circuitBreakerMetrics;

        DisabledState() {
            final int size = circuitBreakerConfig.getRingBufferSizeInClosedState();
            this.circuitBreakerMetrics = new CircuitBreakerMetrics(size);
        }

        /**
         * Returns always true, because the CircuitBreaker is disabled.
         *
         * @return always true, because the CircuitBreaker is disabled.
         */
        @Override
        public boolean tryAcquirePermission() {
            return true;
        }

        /**
         * Does not throw an exception, because the CircuitBreaker is disabled.
         */
        @Override
        public void acquirePermission() {
            // noOp
        }

        @Override
        public void releasePermission() {
            // noOp
        }


        @Override
        public void onError(Throwable throwable) {
            // noOp
        }

        @Override
        public void onSuccess() {
            // noOp
        }

        /**
         * Get the state of the CircuitBreaker
         */
        @Override
        public CircuitBreaker.State getState() {
            return CircuitBreaker.State.DISABLED;
        }

        /**
         * Get metricsof the CircuitBreaker
         */
        @Override
        public CircuitBreakerMetrics getMetrics() {
            return circuitBreakerMetrics;
        }
    }

    private class ForcedOpenState implements CircuitBreakerState {

        private final CircuitBreakerMetrics circuitBreakerMetrics;

        ForcedOpenState() {
            final int size = circuitBreakerConfig.getRingBufferSizeInHalfOpenState();
            this.circuitBreakerMetrics = new CircuitBreakerMetrics(size);
        }

        /**
         * Returns always false, and records the rejected call.
         *
         * @return always false, since the FORCED_OPEN state always denies calls.
         */
        @Override
        public boolean tryAcquirePermission() {
            circuitBreakerMetrics.onCallNotPermitted();
            return false;
        }

        @Override
        public void acquirePermission() {
            circuitBreakerMetrics.onCallNotPermitted();
            throw new CallNotPermittedException(CircuitBreakerStateMachine.this);
        }

        @Override
        public void releasePermission() {
            // noOp
        }

        /**
         * Should never be called when tryAcquirePermission returns false.
         */
        @Override
        public void onError(Throwable throwable) {
            // noOp
        }

        /**
         * Should never be called when tryAcquirePermission returns false.
         */
        @Override
        public void onSuccess() {
            // noOp
        }

        /**
         * Get the state of the CircuitBreaker
         */
        @Override
        public CircuitBreaker.State getState() {
            return CircuitBreaker.State.FORCED_OPEN;
        }

        @Override
        public CircuitBreakerMetrics getMetrics() {
            return circuitBreakerMetrics;
        }
    }

    private class HalfOpenState implements CircuitBreakerState {

        private CircuitBreakerMetrics circuitBreakerMetrics;
        private final float failureRateThreshold;
        private final AtomicInteger testRequestCounter;

        HalfOpenState() {
            this.circuitBreakerMetrics = new CircuitBreakerMetrics(circuitBreakerConfig.getRingBufferSizeInHalfOpenState());
            this.failureRateThreshold = circuitBreakerConfig.getFailureRateThreshold();
            this.testRequestCounter = new AtomicInteger(circuitBreakerConfig.getRingBufferSizeInHalfOpenState());
        }

        /**
         * Checks if test request is allowed.
         * <p>
         * Returns true, if test request counter is not zero.
         * Returns false, if test request counter is zero.
         *
         * @return true, if test request counter is not zero.
         */
        @Override
        public boolean tryAcquirePermission() {
            // 半开状态允许: 10个请求进来进行尝试性测试是否正常
            if (testRequestCounter.getAndUpdate(current -> current == 0 ? current : --current) > 0) {
                return true;
            }

            circuitBreakerMetrics.onCallNotPermitted();
            return false;
        }

        @Override
        public void acquirePermission() {
            // HalfOpenState
            if (!tryAcquirePermission()) {
                throw new CallNotPermittedException(CircuitBreakerStateMachine.this);
            }
        }

        @Override
        public void releasePermission() {
            // 成功则增加 允许进入的请求量
            testRequestCounter.incrementAndGet();
        }

        @Override
        public void onError(Throwable throwable) {
            // CircuitBreakerMetrics is thread-safe
            checkFailureRate(circuitBreakerMetrics.onError());
        }

        @Override
        public void onSuccess() {
            // CircuitBreakerMetrics is thread-safe
            checkFailureRate(circuitBreakerMetrics.onSuccess());
        }

        /**
         * Checks if the current failure rate is above or below the threshold.
         * If the failure rate is above the threshold, transition the state machine to OPEN state.
         * If the failure rate is below the threshold, transition the state machine to CLOSED state.
         *
         * @param currentFailureRate the current failure rate
         */
        private void checkFailureRate(float currentFailureRate) {
            if (currentFailureRate != -1) {
                if (currentFailureRate >= failureRateThreshold) {
                    transitionToOpenState();
                } else {
                    transitionToClosedState();
                }
            }
        }

        /**
         * Get the state of the CircuitBreaker
         */
        @Override
        public CircuitBreaker.State getState() {
            return CircuitBreaker.State.HALF_OPEN;
        }

        @Override
        public CircuitBreakerMetrics getMetrics() {
            return circuitBreakerMetrics;
        }
    }

    private interface CircuitBreakerState {

        boolean tryAcquirePermission();

        void acquirePermission();

        void releasePermission();

        void onError(Throwable throwable);

        void onSuccess();

        CircuitBreaker.State getState();

        CircuitBreakerMetrics getMetrics();

        /**
         * 是否允许发布事件
         * Should the CircuitBreaker in this state publish events
         *
         * @return a boolean signaling if the events should be published
         */
        default boolean shouldPublishEvents(CircuitBreakerEvent event) {
            return event.getEventType().forcePublish || getState().allowPublish;
        }

    }

}
