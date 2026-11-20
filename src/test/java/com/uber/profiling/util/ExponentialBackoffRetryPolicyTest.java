/*
 * Copyright (c) 2026 Harsh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.profiling.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class ExponentialBackoffRetryPolicyTest {
  @Test
  public void interruptedWaitStopsRetriesAndPreservesInterrupt() {
    AtomicInteger calls = new AtomicInteger();
    ExponentialBackoffRetryPolicy<String> policy = new ExponentialBackoffRetryPolicy<>(3, 1);
    try {
      Thread.currentThread().interrupt();
      RuntimeException error = Assert.assertThrows(RuntimeException.class, () -> policy.attempt(() -> {
        if (calls.incrementAndGet() == 1) {
          throw new IllegalStateException("first attempt failed");
        }
        return "unexpected retry";
      }));
      Assert.assertTrue(error.getCause() instanceof InterruptedException);
      Assert.assertTrue(Thread.currentThread().isInterrupted());
      Assert.assertEquals(1, calls.get());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void interruptedWaitDoesNotClearInterruptWhenAllAttemptsFail() {
    AtomicInteger calls = new AtomicInteger();
    ExponentialBackoffRetryPolicy<String> policy = new ExponentialBackoffRetryPolicy<>(3, 1);
    try {
      Thread.currentThread().interrupt();
      Assert.assertThrows(RuntimeException.class, () -> policy.attempt(() -> {
        calls.incrementAndGet();
        throw new IllegalStateException("operation failed");
      }));
      Assert.assertTrue(Thread.currentThread().isInterrupted());
      Assert.assertEquals(1, calls.get());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void retriesNormallyWithoutInterruption() {
    AtomicInteger calls = new AtomicInteger();
    ExponentialBackoffRetryPolicy<String> policy = new ExponentialBackoffRetryPolicy<>(3, 1);
    String result = policy.attempt(() -> {
      if (calls.incrementAndGet() < 3) {
        throw new IllegalStateException("try again");
      }
      return "ok";
    });
    Assert.assertEquals("ok", result);
    Assert.assertEquals(3, calls.get());
    Assert.assertFalse(Thread.currentThread().isInterrupted());
  }

  @Test
  public void exhaustingRetriesRetainsTheOperationFailure() {
    AtomicInteger calls = new AtomicInteger();
    IllegalStateException failure = new IllegalStateException("operation failed");
    ExponentialBackoffRetryPolicy<String> policy = new ExponentialBackoffRetryPolicy<>(2, 1);
    RuntimeException error = Assert.assertThrows(RuntimeException.class, () -> policy.attempt(() -> {
      calls.incrementAndGet();
      throw failure;
    }));
    Assert.assertSame(failure, error.getCause());
    Assert.assertEquals(2, calls.get());
  }
}
