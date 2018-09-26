import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;

public class BkDataload {

    static class ProcessRow extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            String[] words = element.split("\\t" );

            String cookie = words[0];
            String url = words[1];
            String camp = words[2];
            String bkdomain = url.split("\\?")[1];
            String[] bkdomainarray = bkdomain.split("&");
            String bk = bkdomainarray[0].split("=")[1];
            String domain = bkdomainarray[1].split("=")[1];
//            String[] domainarray = domain.split("|");
            String[]  capsplit = camp.split(";");

            for ( String lDomain : domain.split("\\|") ){

                for (String campcatArray : capsplit ) {

                    String[] campcat = campcatArray.split(":");
                    String lcampaign = campcat[0];

                    for (String lCategory : campcat[1].split(",") ){
                         receiver.output( cookie +"\t"+ bk +"\t"+ lDomain + "\t" + lcampaign + "\t" + lCategory );
                        // create and output the table row
//                        TableRow record = new TableRow();
//                        record.set("cookie", cookie);
//                        record.set("bk", bk);
//                        record.set("domain", lDomain);
//                        record.set("camppaign", lcampaign);
//                        record.set("category", lCategory);

                    } //category
                } // Campaign + category
            } // Domain loop
        }
    }

    public static class flattenRow
            extends PTransform<PCollection<String>, PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> bkRecord = lines.apply(ParDo.of(new ProcessRow()));

            // Count the number of times each word occurs.
//            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

            return bkRecord;
        }
    }

    public interface bkOption extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("bkdata.txt")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("bkdata_1.txt")
        String getOutput();
        void setOutput(String value);
    }

    static void runBk(bkOption options) {
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new flattenRow())
                .apply("WriteBlukai", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        bkOption options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(bkOption.class);

        runBk(options);
    }
}
