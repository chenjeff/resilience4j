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
package io.github.resilience4j.ratelimiter;

import io.github.resilience4j.core.lang.NonNullApi;
import io.github.resilience4j.core.lang.NonNullFields;

/**
 * 限流 | 限速
 * 1.timeoutDuration    超时时间    5s
 * 2.limitRefreshPeriod 刷新周期    500ns
 * 3.limitForPeriod     周期允许内访问量    50
 */