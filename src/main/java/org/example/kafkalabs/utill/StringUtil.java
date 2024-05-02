package org.example.kafkalabs.utill;

import java.util.Random;

public class StringUtil {

    private final static Random RANDOM = new Random();

    public static String getRandom(String str1, String str2) {
        if (RANDOM.nextBoolean()) {
            return str1;
        }
        return str2;
    }
}
