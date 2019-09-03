/*
 *
 *  Copyright 2018: Clint Checketts
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
@NonNullApi
@NonNullFields
package io.github.resilience4j.circuitbreaker;

import io.github.resilience4j.core.lang.NonNullApi;
import io.github.resilience4j.core.lang.NonNullFields;

/**
 * 断路器
 * - 参数
 * -- failureRateThreshold          失败率阀值   默认：50%
 * -- waitDurationInOpenState       断路器从OPEN到HALF_OPEN状态等待的时长   默认：60秒
 * -- ringBufferSizeInHalfOpenState 断路器处于HALF_OPEN状态下的ring buffer的大小, 它存储了最近一段时间请求的成功失败状态   默认：10
 * -- ringBufferSizeInClosedState   断路器处于CLOSED状态下的ring buffer的大小，它存储了最近一段时间请求的成功失败状态   默认：100
 * -- recordFailurePredicate        用于判断哪些异常应该算作失败纳入断路器统计   默认：Throwable
 * -- automaticTransitionFromOpenToHalfOpenEnabled  当waitDurationInOpenState时间一过，是否自动从OPEN切换到HALF_OPEN  默认：false
 * <p>
 **/