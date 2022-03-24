import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.relique.jdbc.csv.CsvDriver;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintStream;
import java.sql.*;
import java.util.Map;

public class WordSplitterBolt extends BaseBasicBolt {

    private FileWriter writerFile;

    public String convertResultSetToJson(ResultSet resultSet) throws SQLException {
        if (resultSet == null)
            return null;
        JSONArray json = new JSONArray();
        ResultSetMetaData metadata = resultSet.getMetaData();
        int numColumns = metadata.getColumnCount();
        while (resultSet.next())            //iterate rows
        {
            JSONObject obj = new JSONObject();        //extends HashMap
            for (int i = 1; i <= numColumns; ++i)            //iterate columns
            {
                String column_name = metadata.getColumnName(i);
                obj.put(column_name, resultSet.getObject(column_name));
            }
            json.add(obj);
        }
        return json.toJSONString();
    }

    public void prepare(Map stormConf, TopologyContext context) {
        try {
            String fileName = "/Users/divyanshuvarma/storm-output/output.txt";// output file path - change this path as per your requirement (for windows there is a different style)
            writerFile = new FileWriter(fileName, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple input, BasicOutputCollector collector) {

        String query = input.getString(0);
        String url = "jdbc:relique:csv:" + "/Users/divyanshuvarma/storm-csv"; // folder where your CSVs are - change this path as per your requirement (for windows there is a different style)

        try {
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(query);
            //CsvDriver.writeToCsv(results, System.out, true); //un-comment to print to console
            writerFile.write(convertResultSetToJson(results));
            collector.emit(new Values(results.toString()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("output"));
    }

    public void cleanup() {
        try {
            writerFile.close();
        } catch (Exception e) {
        }
    }
}
