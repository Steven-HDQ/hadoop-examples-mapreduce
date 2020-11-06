package com.opstty;

import com.opstty.job.WordCount;
import com.opstty.q1.Districts;
import com.opstty.q2.Species;
import com.opstty.q3.SpecieCount;
import com.opstty.q4.SpecieMaxHeight;
import com.opstty.q5.SortHeight;
import com.opstty.q6.OldestTree;
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
            programDriver.addClass("speciecount", SpecieCount.class,
                    "A map/reduce program that counts the species in the trees.csv.");
            programDriver.addClass("speciemaxheight", SpecieMaxHeight.class,
                    "A map/reduce program that calculates the height of the tallest tree of each\n" +
                            "specie in the trees.csv.");
            programDriver.addClass("sortheight", SortHeight.class,
                    "A map/reduce program that sorts the trees height from smallest to largest in the trees.csv.");
            programDriver.addClass("oldesttree", OldestTree.class,
                    "A map/reduce program that displays the district where the oldest tree is in the trees.csv.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
