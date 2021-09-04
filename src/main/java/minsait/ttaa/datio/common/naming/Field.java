package minsait.ttaa.datio.common.naming;

import org.apache.spark.sql.Column;

import static minsait.ttaa.datio.common.Common.FIELD;
import static org.apache.spark.sql.functions.col;

public class Field {

    Field(String name) {
        this.name = name;
    }

    private String name = FIELD;

    public String getName() {
        return name;
    }

    public Column column() {
        return col(name);
    }
}
