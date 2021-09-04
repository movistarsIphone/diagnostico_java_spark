package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import minsait.ttaa.datio.common.naming.Field;
//import scala.Enumeration.Val;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Set;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(){

    }

    /** Inicio - MACH **/
    public Dataset<Row> TransformerPuntoCuatro(@NotNull SparkSession spark,Dataset<Row> df) {
        this.spark = spark;
        df = cleanData(df);
        df = rangoEdad(df);
        df = posicionNacionalidad(df);
        df = potencialOverall(df); //Punto 4
        df = columnSelection(df);
        return df;
    }
    /** Fin - MACH **/

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df = readInput();

        df.printSchema();

        df = cleanData(df);
        /** Inicio - MACH **/
        df = rangoEdad(df);
        df = posicionNacionalidad(df);
        df = potencialOverall(df);
        df = filtroAgeNationality(df);
        /** Fin - MACH **/
        
//        df = exampleWindowFunction(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(500, false);
        df.printSchema();

        // Uncomment when you want write your final output
        //write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                /**Inicio - MACH**/
                longName.column(),
                age.column(),
                heightCm.column(),
                weight_kg.column(),
                nationality.column(),
                club_name.column(),
                overall.column(),
                potential.column(),
                /**Fin - MACH**/
//                overall.column(),
//                heightCm.column(),
                teamPosition.column(),
//                catHeightByPosition.column()
                /**Inicio - MACH**/
                age_range.column(),
                rank_by_nationality_position.column(),
                potential_vs_overall.column()
                /**Fin - MACH**/
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }
    
    /**Inicio - MACH**/
    /**
     * @param df es un Dataset con informacion de los jugadores (debe tener las columnas team_position y edad)
     * @return agregar al Dataset la columna "age_range"
     */
    private Dataset<Row> rangoEdad(Dataset<Row> df) {
        WindowSpec w = Window.partitionBy(teamPosition.column())
                .orderBy(age.column().asc());

        Column rank = rank().over(w);

        Column rule = when(rank.$greater$eq(32), "D")
        		.when(rank.between(27, 31), "C")
        		.when(rank.between(23, 26), "B")
        		.when(rank.$less(23), "A");
        
    	df = df.withColumn(age_range.getName(), rule);
        return df;
    }

    /**
     * @param df es un Dataset con informacion de los jugadores (debe tener las columnas nationality, teamPosition y overall)
     * @return agregar al Dataset la columna "rank_by_nationality_position"
     */
    private Dataset<Row> posicionNacionalidad(Dataset<Row> df) {
        WindowSpec w = Window.partitionBy(nationality.column())
        		.partitionBy(teamPosition.column())
                .orderBy(overall.column().desc());
        
    	df = df.withColumn(rank_by_nationality_position.getName(), row_number().over(w));
        return df;
    }

    /**
     * @param df es un Dataset con informacion de los jugadores
     * @return agregar al Dataset la columna "potential_vs_overall"
     */
    private Dataset<Row> potencialOverall(Dataset<Row> df) {        
    	df = df.withColumn(potential_vs_overall.getName(), col("potential").divide(col("overall")));
        return df;
    }
    
    /**
     * @param df es un Dataset con informacion de los jugadores (debe tener las columnas team_position y edad)
     * @return agregar al Dataset la columna "age_range"
     */
    private Dataset<Row> filtroAgeNationality(Dataset<Row> df) {       
        df.where(df.col("rank_by_nationality_position").leq(3));
        df.where(df.col("age_range").isin("B","C")).where(df.col("potential_vs_overall").geq(1.15));
        df.where(df.col("age_range").isin("A")).where(df.col("potential_vs_overall").geq(1.25));
        df.where(df.col("age_range").isin("D")).where(df.col("rank_by_nationality_position").leq(5));
        return df;
    }
    
    /**Fin - MACH**/

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);
        return df;
    }

}
