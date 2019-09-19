package com.koverse.examples.integration;

import java.util.Random;

public class DummySecondFactorGenerator {

  private static final long TWO_FA_SECRET = 1234567890;

  public static int generateSequenceNumber() {

    Long timestamp = System.currentTimeMillis() / 30_000; // 30 second increments

    Random random = new Random(DummySecondFactorGenerator.TWO_FA_SECRET + timestamp);

    return Math.abs(random.nextInt());
  }

  public static void main(String[] args) {
    System.out.println(generateSequenceNumber());
  }
}
