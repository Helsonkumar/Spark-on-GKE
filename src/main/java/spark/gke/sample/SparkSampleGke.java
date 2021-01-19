
package spark.gke.sample;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkSampleGke {

    public static void main(String[] args){

        SparkSession spark = SparkSession.builder().appName("SparkSampleGke").getOrCreate();

        System.out.println("You are using Spark " + spark.version());
        spark.sparkContext().setLogLevel("ERROR");
        List<Row> list=new ArrayList<Row>();
        list.add(RowFactory.create("one"));
        list.add(RowFactory.create("two"));
        list.add(RowFactory.create("three"));
        list.add(RowFactory.create("four"));
        List<org.apache.spark.sql.types.StructField> listOfStructField=new ArrayList<org.apache.spark.sql.types.StructField>();
        listOfStructField.add(DataTypes.createStructField("test", DataTypes.StringType, true));
        StructType structType=DataTypes.createStructType(listOfStructField);
        Dataset<Row> data=spark.createDataFrame(list,structType);
        data.show();
        System.out.println("This is the end of Code");
        System.out.println("You took this time to run the job Helson..:)");

        spark.stop();

    }


}
