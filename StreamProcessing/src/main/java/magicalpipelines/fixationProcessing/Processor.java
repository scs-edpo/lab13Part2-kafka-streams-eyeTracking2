package magicalpipelines.fixationProcessing;

import java.util.Random;

public class Processor {

  /*
  This method should translate the xPos and yPos of fixations into AOIs.
  For sake of simulation, in this example, the AOIs are returned randomly
   */
  public static AoiNames findAOI(double xPos, double yPos) {
    Random rand = new Random();
    return AoiNames.values()[rand.nextInt(AoiNames.values().length)];
  }



}
