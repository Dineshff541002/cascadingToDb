package co.dwit;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.jdbc.JDBCFactory;
import cascading.jdbc.JDBCScheme;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import java.lang.reflect.Type;
import java.util.Properties;

import static cascading.jdbc.JDBCFactory.PROTOCOL_JDBC_USER;

public class CascadingToDb {
    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    private static final String TABLE_NAME = "emp_count";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final String DB_URL = "jdbc:mysql://localhost/tutorial";
    private static Fields sinkFields;

    //defining delimiters and file locations

    private static String DELIMITER_SOURCE = ",";
    private static String DELIMITER_SINK = "|";
    private static String FILE_SOURCE  = "/cascadingIntro/cascading_intro_file.csv";
    private static String FILE_SINK = "/cascading_To_Db";
    private static FlowDef flowDef = new FlowDef();

    public static void main(String[] args) {
//        FILE_SOURCE = args[0];
//        FILE_SINK = args[1];
        //fields that match our data layout
        Fields sourceFields = new Fields("id", "first_name", "last_name", "email", "gender", "salary");
        sinkFields = new Fields(new Comparable[]{"gender", "count"}, new Type[]{String.class, Integer.class});
        //source and sink taps definition
        Tap sourceTap = new Hfs(new TextDelimited(sourceFields, true, DELIMITER_SOURCE), FILE_SOURCE);
        Tap sinkTap = new Hfs(new TextDelimited(sinkFields, false, DELIMITER_SINK), FILE_SINK, SinkMode.REPLACE);
        Pipe pipe = new Pipe("personData");
        // counting each kind of gender
        pipe = new CountBy(pipe, new Fields("gender"), new Fields("count"));
        pipe = new Each(pipe, new Debug("pipeLayout", true));
        Pipe pipe_db = new Pipe("dbPipe", pipe);

        //completing the flow by adding multiple sinks

        flowDef.addSource(pipe, sourceTap);
        flowDef.addTailSink(pipe, sinkTap);
        flowDef.addTailSink(pipe_db, getJDBCSinkTap());
        Flow flow = new HadoopFlowConnector().connect(flowDef);
        flow.complete();

    }
    private static  Tap getJDBCSinkTap(){
        Properties schemePros = new Properties();
        Properties tapProps = new Properties();

        // setting properties required by jdbc factory
        tapProps.put(JDBCFactory.PROTOCOL_JDBC_USER,DB_USER);
        tapProps.put(JDBCFactory.PROTOCOL_JDBC_PASSWORD,DB_PASS);
        tapProps.setProperty(JDBCFactory.PROTOCOL_TABLE_NAME,TABLE_NAME);
        tapProps.setProperty(JDBCFactory.PROTOCOL_JDBC_DRIVER,DRIVER_NAME);
        tapProps.put(JDBCFactory.PROTOCOL_PRIMARY_KEYS,"emp_count_id");
        JDBCFactory factory = new JDBCFactory();

        // createing jdbc scheme
        JDBCScheme updateScheme = (JDBCScheme) factory.createScheme("genderWrite",sinkFields,schemePros);
        return  factory.createTap("jdbc",updateScheme,DB_URL,SinkMode.UPDATE, tapProps);
    }
}
