package com.skemu.reactor;

public class MemLeakApp {

  public static void main(String... args) {
    new MemLeakRunner(Integer.MAX_VALUE).run();
  }
}
