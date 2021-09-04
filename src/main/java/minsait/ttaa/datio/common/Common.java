package minsait.ttaa.datio.common;

import minsait.ttaa.datio.common.naming.Field;

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
    public static final String A ="A";
    public static final String B ="B";
    public static final String C ="C";
    public static final String D ="D";

    public static final String PRUEBA_UNITARIA_4 = "Prueba unitaria del metodo 4\n";
    public static final String FIELD ="field";

    public static final String SHORT_NAME = "short_name";
    public static final String LONG_NAME = "long_name";
    public static final String AGE = "age";
    public static final String WEIGTH_KG = "weight_kg";
    public static final String NATIONALITY = "nationality";
    public static final String CLUB_NAME = "club_name";
    public static final String POTENTIAL = "potential";
    public static final String OVERALL = "overall";
    public static final String TEAM_POSITION = "team_position";
    public static final String HEIGHT_CM = "height_cm";

    public static final String CAT_HEIGHT_BY_POSITION = "cat_height_by_position";
    public static final String AGE_RANGE = "age_range";
    public static final String RANK_BY_NATIONALITY_POSITION = "rank_by_nationality_position";
    public static final String POTENTIAL_VS_OVERALL = "potential_vs_overall";

    public static String USER_DIR = "user.dir";
    public static String DIR_PARAMS = "\\src\\test\\resources\\params";
    public static String INPUT_PATH;
    public static String OUTPUT_PATH;

    public Common() throws IOException {

        String directorioActual = System.getProperty(USER_DIR);

        try (Scanner s = new Scanner(new File(directorioActual+DIR_PARAMS))) {
            INPUT_PATH = s.next();
            OUTPUT_PATH = s.next();
        }
    }
    /** Fin - MACH **/
}
