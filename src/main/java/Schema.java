import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import io.github.cdimascio.dotenv.Dotenv;

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
        String DATASET  = WordcountPipeline.getEnv().get("DATASET");
        String PROJECT_ID =  WordcountPipeline.getEnv().get("PROJECT_ID");

        TableReference table = new TableReference();
        table.setDatasetId(DATASET);
        table.setProjectId(PROJECT_ID);
        table.setTableId("wordcount");
        return table;
    }
}
