/*
 *
 *  Copyright 2017: Robert Winkler
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
package io.github.resilience4j.core;

import io.github.resilience4j.core.lang.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class EventProcessor<T> implements EventPublisher<T> {

    /**
     * 是否存在事件注册
     */
    private boolean consumerRegistered;

    /**
     * 全类型-事件
     */
    List<EventConsumer<T>> onEventConsumers = new CopyOnWriteArrayList<>();

    /**
     * 特定类型-事件
     */
    ConcurrentMap<String, List<EventConsumer<T>>> eventConsumerMap = new ConcurrentHashMap<>();

    public boolean hasConsumers() {
        return consumerRegistered;
    }

    /**
     * 特定类型-事件, 注册入口
     *
     * @param className
     * @param eventConsumer
     */
    @SuppressWarnings("unchecked")
    public synchronized void registerConsumer(String className, EventConsumer<? extends T> eventConsumer) {
        this.consumerRegistered = true;
        this.eventConsumerMap.compute(className, (k, consumers) -> {
            if (consumers == null) {
                consumers = new ArrayList<>();
                consumers.add((EventConsumer<T>) eventConsumer);
                return consumers;
            } else {
                consumers.add((EventConsumer<T>) eventConsumer);
                return consumers;
            }
        });
    }

    /**
     * 触发事件
     *
     * @param event
     * @param <E>
     * @return
     */
    public <E extends T> boolean processEvent(E event) {
        boolean consumed = false;
        if (!onEventConsumers.isEmpty()) {
            onEventConsumers.forEach(onEventConsumer -> onEventConsumer.consumeEvent(event));
            consumed = true;
        }

        if (!eventConsumerMap.isEmpty()) {
            List<EventConsumer<T>> eventConsumers = this.eventConsumerMap.get(event.getClass().getSimpleName());
            if (eventConsumers != null && !eventConsumers.isEmpty()) {
                eventConsumers.forEach(consumer -> consumer.consumeEvent(event));
                consumed = true;
            }
        }

        return consumed;
    }

    /**
     * 全类型-事件, 注册入口
     *
     * @param onEventConsumer
     */
    @Override
    public synchronized void onEvent(@Nullable EventConsumer<T> onEventConsumer) {
        this.consumerRegistered = true;
        this.onEventConsumers.add(onEventConsumer);
    }

}
