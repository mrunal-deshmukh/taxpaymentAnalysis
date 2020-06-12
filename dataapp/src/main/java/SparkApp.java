import org.apache.commons.cli.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkApp {

    public static class DataFilter implements Function<String, Boolean> {
        private static int DATA_COLUMNS = 6;

        @Override
        public Boolean call(String value) throws Exception {
            if (value.length() == 0) {
                return false;
            }

            String[] csvValues = value.split(",");
            if (csvValues.length != DATA_COLUMNS) {
                return false;
            }

            for (String csvValue : csvValues) {
                if (csvValue.trim().length() == 0) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class DataJoiner implements Function<String, String> {

        private Map<String, CityAndData> zipToCityState;
        private static String COMMA = ",";

        public DataJoiner(Map<String, CityAndData> zipToCityState) {

            this.zipToCityState = zipToCityState;
        }

        @Override
        /**                0 ID                                ,   1 (NAME), 2(TAXAMOUNT) , 3(YEAR) , 4(ZIPCODE) ,5(NO OF HOUSEHOLDS)
         * @param value 342a16a2-0235-42ac-ab1a-60da81daac60,Victor Scally,     81416,       2000,     98933,        7386
         */
        public String call(String value) throws Exception {
            String[] dataRowSplit = value.split(",");
            if (dataRowSplit.length == 6) {
                String zipCode = dataRowSplit[4];
                CityAndData cityAndData = zipToCityState.get(zipCode);
                if (cityAndData != null) {
                    return cityAndData.getState() + COMMA
                            + cityAndData.getCity() + COMMA +
                            dataRowSplit[2] + COMMA +
                            dataRowSplit[3] + COMMA +
                            dataRowSplit[4] + COMMA +
                            dataRowSplit[5];
                }
            }
            return null;
        }
    }

    public static void main(String[] args) throws ParseException, IOException {
        Options options = new Options();
        options.addOption("i", true, "provide hdfs url to read input data files");
        options.addOption("o", true, "provide hdfs url to an output folder, the folder will be auto created");
        options.addOption("c", true, "provide hdfs url to csv file for reading uszips.csv data");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("i") && cmd.hasOption("o") && cmd.hasOption("c")) {
            SparkConf conf = new SparkConf().setAppName("SparkApp");
            JavaSparkContext sc = new JavaSparkContext(conf);

            String hdfsInputPath = cmd.getOptionValue("i");
            String outputFolderPath = cmd.getOptionValue("o");
            String csvFile = cmd.getOptionValue("c");
            FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
            fs.delete(new Path(outputFolderPath), true);

            JavaRDD<String> cachedCSVFile = sc.textFile(csvFile).cache();
            Long lineCount = cachedCSVFile.count();
            Map<String, CityAndData> zipToCityState = cachedCSVFile.map((Function<String, Tuple2<String, CityAndData>>) csvRow -> {
                String[] row = csvRow.split(",");
                return new Tuple2<>(row[0], new CityAndData(row[1], row[2]));
            }).take(lineCount.intValue()).stream().collect(Collectors.toMap((v) -> v._1, (v) -> v._2));

            sc.textFile(hdfsInputPath)
                    .filter(new DataFilter()).map(line -> line)
                    .map(new DataJoiner(zipToCityState))
                    .filter((Function<String, Boolean>) outPutLine -> outPutLine != null && outPutLine.length() > 0)
                    .saveAsTextFile(outputFolderPath);
            sc.stop();
        }

    }
}
