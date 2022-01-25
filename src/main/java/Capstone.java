import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class Capstone {

    public static void main(String[] args) {

        DataflowPipelineOptions dataflowOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        dataflowOptions.setProject("york-cdf-start");
        dataflowOptions.setRegion("us-central1");
        dataflowOptions.setJobName("jtcapjava");
        dataflowOptions.setRunner(DataflowRunner.class);

        Pipeline p = Pipeline.create(dataflowOptions);

        TableSchema views_schema =
                new TableSchema()
                        .setFields(
                                Arrays.asList(
                                        new TableFieldSchema()
                                                .setName("cust_tier_code")
                                                .setType("STRING")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("sku")
                                                .setType("INTEGER")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("total_no_of_product_views")
                                                .setType("INTEGER")
                                                .setMode("REQUIRED")));

        TableSchema sales_schema =
                new TableSchema()
                        .setFields(
                                Arrays.asList(
                                        new TableFieldSchema()
                                                .setName("cust_tier_code")
                                                .setType("STRING")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("sku")
                                                .setType("INTEGER")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("total_sales_amount")
                                                .setType("FLOAT")
                                                .setMode("REQUIRED")));

        PCollection<TableRow> views_rows =
                p.apply(
                        "Read from BigQuery1",
                        BigQueryIO.readTableRows()
                                .fromQuery(
                                        "SELECT customers.CUST_TIER_CODE AS cust_tier_code, product_views.SKU AS sku, COUNT(DISTINCT(product_views.EVENT_TM)) AS total_no_of_product_views \n" +
                                                "FROM `york-cdf-start.final_input_data.customers` AS customers \n" +
                                                "JOIN `york-cdf-start.final_input_data.product_views` AS product_views ON (customers.CUSTOMER_ID = product_views.CUSTOMER_ID) \n" +
                                                "GROUP BY customers.CUST_TIER_CODE, product_views.SKU")
                                .usingStandardSql());

        PCollection<TableRow> sales_rows =
                p.apply(
                        "Read from BigQuery2",
                        BigQueryIO.readTableRows()
                                .fromQuery(
                                        "SELECT customers.CUST_TIER_CODE AS cust_tier_code, orders.SKU AS sku, ROUND(SUM(orders.ORDER_AMT), 2) AS total_sales_amount\n" +
                                                "FROM `york-cdf-start.final_input_data.customers` AS customers\n" +
                                                "JOIN `york-cdf-start.final_input_data.orders` AS orders ON (customers.CUSTOMER_ID = orders.CUSTOMER_ID)\n" +
                                                "GROUP BY customers.CUST_TIER_CODE, orders.SKU")
                                .usingStandardSql());

        views_rows.apply(
                "Write to BigQuery1",
                BigQueryIO.writeTableRows()
                        .to("york-cdf-start:final_james_thomson.cust_tier_code-sku-total_no_of_product_views_java")
                        .withSchema(views_schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        sales_rows.apply(
                "Write to BigQuery2",
                BigQueryIO.writeTableRows()
                        .to("york-cdf-start:final_james_thomson.cust_tier_code-sku-total_sales_amount_java")
                        .withSchema(sales_schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        p.run().waitUntilFinish();


    }
}

// adding comment to test Jenkins Maven project version