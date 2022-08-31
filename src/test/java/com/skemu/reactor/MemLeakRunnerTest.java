package com.skemu.reactor;

import org.junit.jupiter.api.Test;

class MemLeakRunnerTest {


  @Test
  void memLeakRunnerTest() {
    new MemLeakRunner(Integer.MAX_VALUE).run();
  }
}
