/*-
 * -\-\-
 * Flo Runner
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.context;

import static com.spotify.flo.context.FloRunner.runTaskAndExit;
import static com.spotify.flo.context.FloRunner.runTaskAsync;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.spotify.flo.Task;
import com.spotify.flo.status.NotReady;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class FloRunnerTest {

  private static final Task<String> NOT_READY = Task.named("notReady").ofType(String.class)
      .process(() -> {
        throw new NotReady();
      });

  @Test
  public void nonBlockingRunnerDoesNotBlock() {
    final CompletableFuture<String> future = new CompletableFuture<>();
    final Task<String> foo = Task.named("foo").ofType(String.class)
        .process(() -> {
          try {
            Thread.sleep(10);
            future.complete("foo");
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return "foo";
        });

    runTaskAsync(foo);

    assertThat(future.isDone(), is(false));
  }

  @Test
  public void blockingRunnerBlocks() {
    final CompletableFuture<String> future = new CompletableFuture<>();
    final Task<String> foo = Task.named("foo").ofType(String.class)
        .process(() -> {
          try {
            Thread.sleep(10);
            future.complete("foo");
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return "foo";
        });

    runTaskAndExit(foo, status -> { });

    assertThat(future.isDone(), is(true));
  }

  @Test
  public void valueIsPassedInFuture() throws Exception {
    final Task<String> foo = Task.named("foo").ofType(String.class)
        .process(() -> "foo");

    final String result = runTaskAsync(foo).get(1, TimeUnit.SECONDS);

    assertThat(result, is("foo"));
  }

  @Test
  public void valuesArePassedToDependents() throws Exception {
    final Task<String> bar = Task.named("bar").ofType(String.class)
        .process(() -> "bar");

    final Task<String> foo = Task.named("foo").ofType(String.class)
        .in(() -> bar)
        .process((b) -> "foo" + b);

    final String result = runTaskAsync(foo).get(1, TimeUnit.SECONDS);

    assertThat(result, is("foobar"));
  }

  @Test
  public void exceptionsArePassed() throws Exception {
    try {
      runTaskAsync(NOT_READY).get(1, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assert e.getCause() instanceof NotReady;
    }
  }

  @Test
  public void exceptionsArePassedThroughDag() throws Exception {
    final Task<String> foo = Task.named("foo").ofType(String.class)
        .in(() -> NOT_READY)
        .process((b) -> "foo" + b);

    try {
      runTaskAsync(foo).get(1, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assert e.getCause() instanceof NotReady;
    }
  }

  @Test
  public void exceptionsExitNonZero() throws Exception {
    final Task<String> throwingTask = Task.named("throwingTask").ofType(String.class)
        .process(() -> {
          throw new RuntimeException("this task should throw");
        });

    final CompletableFuture<Integer> future = new CompletableFuture<>();

    runTaskAndExit(throwingTask, future::complete);

    final int status = future.get(1, TimeUnit.SECONDS);

    assertThat(status, is(1));
  }
}
