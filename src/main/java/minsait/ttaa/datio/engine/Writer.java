package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import minsait.ttaa.datio.common.Common;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.shortName;
import static minsait.ttaa.datio.common.naming.PlayerInput.age;
import static org.apache.spark.sql.SaveMode.Overwrite;

import java.io.IOException;

abstract class Writer {

	static void write(Dataset<Row> df) {

//    	df
//        .coalesce(2)
//        .write()
//        .partitionBy(teamPosition.getName())
//        .mode(Overwrite)
//        .parquet(OUTPUT_PATH);

		/** Inicio - MACH **/

		try {
			new Common();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		df
				.coalesce(2)
				.write()
				.partitionBy(shortName.getName())
				.partitionBy(age.getName())
				.mode(Overwrite)
				.parquet(OUTPUT_PATH);
		/** Fin - MACH **/
	}

}
