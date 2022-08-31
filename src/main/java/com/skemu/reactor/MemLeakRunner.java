package com.skemu.reactor;

import java.util.stream.Stream;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class MemLeakRunner implements Runnable {

  private final int nrOfStatements;

  public MemLeakRunner(int nrOfStatements) {
    this.nrOfStatements = nrOfStatements;
  }

  @Override
  public void run() {
    Flux.just("a", "b", "c", "d", "e", "f", "g")
        .flatMap(id -> generateTuplesFor(id, (nrOfStatements / 7)))
        .parallel(6)
        .runOn(Schedulers.parallel())
        .flatMap(this::generatePairsFor)
        .map(this::mapPair)
        .sequential()
        .map(this::mapPair) // If you remove this call, it seems to run without leaking.
        .blockLast();
  }

  private Flux<Tuple2<String, String>> generateTuplesFor(String id, int amount) {
    return Flux.fromStream(Stream.iterate(0, i -> i + 1)
        .limit(amount)
        .map(i -> generateTupleFor(id, i)));
  }

  private Tuple2<String, String> generateTupleFor(String id, int number) {
    return Tuples.of(String.format("left-%s-%s", id, number), String.format("right-%s-%s", id, number));
  }

  private Flux<Pair> generatePairsFor(Tuple2<String, String> tuple2) {
    return Flux.just(new Pair(tuple2.getT1(), tuple2.getT2()));
  }

  private Pair mapPair(Pair pair) {
    var left = transformValue(pair.getLeft());
    var right = transformValue(pair.getRight());
    return new Pair(left, right);
  }

  private String transformValue(String value) {
    return String.format("transformed-%s", value);
  }

  private static class Pair {
    private final String left;
    private final String right;

    Pair(String left, String right) {
      this.left = left;
      this.right = right;
    }

    String getLeft() {
      return left;
    }

    String getRight() {
      return right;
    }
  }
}
