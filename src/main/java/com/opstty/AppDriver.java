package com.opstty;

import com.opstty.job.WordCount;
import com.opstty.q1.Districts;
import com.opstty.q2.Species;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districts", Districts.class,
                    "A map/reduce program that lists the districts in the trees.csv.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that lists the species in the trees.csv.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
