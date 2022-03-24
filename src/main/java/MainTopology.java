import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("File-Reader-Spout", new FileReaderSpout());
        builder.setBolt("Simple-Bolt", new WordSplitterBolt()).shuffleGrouping("File-Reader-Spout");

        //Configuration
        Config conf = new Config();
        conf.put("fileToRead", "/Users/divyanshuvarma/storm-sql/sql.txt");// this file contains your SQL query - change this path as per your requirement (for windows there is a different style)
        conf.put("dirToWrite", "/Users/divyanshuvarma/storm-output/");// path to your output folder - change this path as per your requirement (for windows there is a different style)

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("file-write-test", conf, builder.createTopology());
            Thread.sleep(10000);
        } finally {
            cluster.shutdown();
        }
    }
}
