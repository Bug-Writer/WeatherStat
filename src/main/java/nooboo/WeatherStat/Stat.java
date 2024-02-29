package nooboo.WeatherStat;

import nooboo.WeatherStat.Sink.DatabaseSink;
import nooboo.WeatherStat.Sink.MySQLSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Stat {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("weatherInfo")
                .setGroupId("mainConsumer")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Data Storage
        DatabaseSink mySQLSink = new MySQLSink();
        stream.addSink(new DatabaseSinkFunction(Configuration.createInstance()));
        // Data Transformation
        stream.print();

        env.execute("Weather Data Processing");
    }

}
