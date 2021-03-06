/*-
 * -\-\-
 * Flo Workflow Definition
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.flo;

import static java.util.stream.Collectors.toList;

import com.spotify.flo.EvalContext.Value;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Internal utility functions for the {@link TaskBuilder} api implementation
 */
class BuilderUtils {

  private BuilderUtils() {
  }

  static <F, Z> ChainingEval<F, Z> leafEvalFn(EvalClosure<Fn1<F, Value<Z>>> fClosure) {
    return new ChainingEval<>(fClosure);
  }

  static <R> EvalClosure<R> gated(TaskId taskId, Fn<R> code) {
    return ec -> ec.invokeProcessFn(taskId, () -> ec.value(code));
  }

  static <R> EvalClosure<R> gatedVal(
      TaskId taskId,
      Fn1<EvalContext, Value<R>> code) {
    return ec -> ec.invokeProcessFn(taskId, () -> code.apply(ec));
  }

  /**
   * Converts an array of {@link Fn}s of {@link Task}s to a {@link Fn} of a list of
   * those tasks {@link Task}s.
   *
   * It will only evaluate the functions (through calling {@link Fn#get()})
   * when the returned function is invoked. Thus it retains laziness.
   *
   * @param tasks  An array of lazy evaluated tasks
   * @return A function of a list of lazily evaluated tasks
   */
  @SafeVarargs
  static Fn<List<Task<?>>> lazyList(Fn<? extends Task<?>>... tasks) {
    return () -> Stream.of(tasks)
        .map(Fn::get)
        .collect(toList());
  }

  @SafeVarargs
  static <T> Fn<List<T>> lazyFlatten(Fn<? extends List<? extends T>>... lists) {
    return () -> Stream.of(lists)
        .map(Fn::get)
        .flatMap(List::stream)
        .collect(toList());
  }

  static <T> List<T> appendToList(List<T> list, T t) {
    final List<T> newList = new ArrayList<>(list);
    newList.add(t);
    return newList;
  }

  static final class ChainingEval<F, Z> implements Serializable {

    private final EvalClosure<Fn1<F, Value<Z>>> fClosure;

    ChainingEval(EvalClosure<Fn1<F, Value<Z>>> fClosure) {
      this.fClosure = fClosure;
    }

    EvalClosure<Z> enclose(F f) {
      return taskContext -> fClosure.eval(taskContext).flatMap(ff -> ff.apply(f));
    }

    <G> ChainingEval<G, Z> chain(EvalClosure<Fn1<G, F>> mapClosure) {
      EvalClosure<Fn1<G, Value<Z>>> continuation = ec -> {
        Value<Fn1<G, F>> gv = mapClosure.eval(ec);
        Value<Fn1<F, Value<Z>>> fv = fClosure.eval(ec);

        return Values.mapBoth(ec, gv, fv, (gc, fc) -> g -> fc.apply(gc.apply(g)));
      };
      return new ChainingEval<>(continuation);
    }
  }
}
