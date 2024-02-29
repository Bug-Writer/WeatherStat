package nooboo.WeatherStat;

import nooboo.WeatherStat.Sink.DatabaseSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class DatabaseSinkFunction implements SinkFunction<String> {

    private DatabaseSink dbSink;

    public DatabaseSinkFunction(DatabaseSink dbSink) {
        this.dbSink = dbSink;
    }

    @Override
    public void invoke(String value, Context context) {
        dbSink.save(value);
    }
}
