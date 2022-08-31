package com.skemu.reactor;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

class MemLeakTest {

  @Test
  void memLeakTest() {
    Flux.just("a", "b", "c", "d", "e", "f", "g")
        .flatMap(id -> generateTuplesFor(id, (Integer.MAX_VALUE / 7)))
        .parallel(6)
        .runOn(Schedulers.parallel())
        .flatMap(this::generatePairsFor)
        .map(this::mapPair)
        .sequential()
        .map(this::mapPair) // If you remove this call, it seems to run without leaking.
//        .map(this::toTriple) // Commenting out the above line and just mapping to a different object doesn't seem to leak.
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

  private Triple toTriple(Pair pair) {
    return new Triple(pair.left, pair.getLeft() + pair.getRight(), pair.getRight());
  }

  private String transformValue(String value) {
    return String.format("transformed-%s", value);
  }

  static class Pair {
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

  static class Triple {
    private final String left;

    private final String middle;

    private final String right;

    Triple(String left, String middle, String right) {
      this.left = left;
      this.middle = middle;
      this.right = right;
    }

    String getLeft() {
      return left;
    }

    String getMiddle() {
      return middle;
    }

    String getRight() {
      return right;
    }
  }
}
