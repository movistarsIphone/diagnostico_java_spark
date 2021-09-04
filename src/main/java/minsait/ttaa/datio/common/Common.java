package minsait.ttaa.datio.common;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public final class Common {

    public static final String SPARK_MODE = "local[*]";
    public static final String HEADER = "header";
    public static final String INFER_SCHEMA = "inferSchema";
//    public static final String INPUT_PATH = "src/test/resources/data/players_21.csv";
//    public static final String OUTPUT_PATH = "src/test/resources/data/output";
    /** Inicio - MACH **/
    public static String INPUT_PATH;
    public static String OUTPUT_PATH;

    public Common() throws IOException {

        String directorioActual = System.getProperty("user.dir");

        try (Scanner s = new Scanner(new File(directorioActual+"\\src\\test\\resources\\params"))) {
            INPUT_PATH = s.next();
            OUTPUT_PATH = s.next();
        }
    }
    /** Fin - MACH **/
}
