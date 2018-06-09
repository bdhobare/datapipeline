import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;


import java.util.ArrayList;
import java.util.List;

public class Schema {

    public static TableSchema getSchema(){
        // event schema for the raw_events table
        List<TableFieldSchema> fields = new ArrayList();
        fields.add(new TableFieldSchema().setName("Word").setType("STRING"));
        fields.add(new TableFieldSchema().setName("Count").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);

        return schema;
    }
    public static TableReference getTable(){
        // BQ output table information
        String DATASET  = "wordcount";
        String PROJECT_ID =  "PROJECT_ID";

        TableReference table = new TableReference();
        table.setDatasetId(DATASET);
        table.setProjectId(PROJECT_ID);
        table.setTableId("wordcount");
        return table;
    }
}
