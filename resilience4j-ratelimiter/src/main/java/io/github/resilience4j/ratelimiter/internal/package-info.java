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
package io.github.resilience4j.ratelimiter.internal;

import io.github.resilience4j.core.lang.NonNullApi;
import io.github.resilience4j.core.lang.NonNullFields;

/**
 * 限流
 * <p>
 * --- 平滑限流
 * 1.令牌桶限流算法
 * 2.漏桶限流算法
 * --- 粗暴限流
 * 3.固定并发数限流算法
 * <p>
 * --- 实现
 * 令牌桶限流
 * - 堵塞
 * - 非堵塞
 * -- 令牌桶限流的好处是可以应对突发请求的流量。
 * 固定并发数限流
 * - 信号量
 */