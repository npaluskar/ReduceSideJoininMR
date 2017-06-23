import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author npaluskar 
 * Purpose - Find total sales by state by performing reduce side join
 *
 */

public class TotalSalesByState {
	
//output of 1st MR job - for each customer id - state and its sales price
	private static final String OUTPUT_PATH = "intermediate_output";

//	Mapper for customer file - output format (customer_id,state)
	public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String customerRow = value.toString();
			String customerData[] = customerRow.split("\t");
			Text customer_id = new Text(customerData[0]);
//			Indicate the field is coming from customer file
			Text state = new Text("customer\t" + customerData[4]);
			context.write(customer_id, state);
		}
	}
// Mapper for Sales file - output format (customer_id,sales price)
	public static class SalesMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String salesRow = value.toString();
			String salesData[] = salesRow.split("\t");
			Text customerId = new Text(salesData[0]);
//			Indicate the field is coming from sales file
			Text sales_price = new Text("sales\t" + salesData[1]);
			context.write(customerId, sales_price);
		}

	}

//	Reducer for performing join of two files customer and sales.
	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double total_sales = 0.0;
			String state = "";
			for (Text t : values) {
				String data[] = t.toString().split("\t");
				if (data[0].equals("customer")) {
					state = data[1];
				} else if (data[0].equals("sales")) {
					total_sales = Float.parseFloat(data[1]);
				}
			}
			String total = String.format("%f", total_sales);
			context.write(new Text(state), new Text(total));
		}
	}
// Input to States Mapper is (state,sales price) . output is (state,sales price)
	public static class StatesMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String statesRow = value.toString();
			String statesData[] = statesRow.split("\t");
			Text state = new Text(statesData[0]);
			Text sales = new Text(statesData[1]);
			context.write(state, sales);
		}
	}
	// Input to States Reduce is (state,sales price) . output is (state,total_sales price)
	public static class StatesReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double total_sales = 0.0;
			for (Text t : values) {
				String sale = t.toString();
				total_sales += Float.parseFloat(sale);
			}
			String total = String.format("%f", total_sales);
			context.write(new Text(key), new Text(total));
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

//		setting job configuration for 1st MR job which will perform join of two files
		Job job1 = new Job(conf, "Total Sales By Customer ID");
		job1.setJarByClass(TotalSalesByState.class);

		job1.setReducerClass(ReduceJoinReducer.class);
		job1.setOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);

		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, SalesMapper.class);

		Path outputFilePath = new Path(OUTPUT_PATH);

		FileOutputFormat.setOutputPath(job1, outputFilePath);

		int exitCode;
		if (job1.waitForCompletion(true)) {
			exitCode = 0;
		} else {
			exitCode = 1;
		}
//		setting job configuration for 2nd MR job which will perform aggregation according to state
		if (exitCode == 0) {
			Job job2 = new Job(conf, "Total Sales By State");
			job2.setJarByClass(TotalSalesByState.class);
			job2.setMapperClass(StatesMapper.class);
			job2.setReducerClass(StatesReducer.class);
			job2.setOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);

			TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));

			Path outputFilePath2 = new Path(args[2]);

			FileOutputFormat.setOutputPath(job2, outputFilePath2);
			outputFilePath2.getFileSystem(conf).delete(outputFilePath2);

			int exitCode1;
			if (job2.waitForCompletion(true)) {
				exitCode1 = 0;
			} else {
				exitCode1 = 1;
			}
			System.exit(exitCode);
		}
	}

}
