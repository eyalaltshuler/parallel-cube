package cube;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import infra.CubeGroup;
import infra.Element;

import java.io.IOException;
import java.util.Random;
import java.util.TreeMap;


public class CubeRoundOneMapper extends Mapper<LongWritable,Text,BytesWritable,NullWritable>{
	
	private double prob;
	private Random rand = null;
	
	private CubeGroup retrieveKey(String line) {
		line = line.trim();
		String[] dataAsStr = line.split(" ");
		TreeMap<Integer,Element> values = new TreeMap<Integer,Element>();
		int length = dataAsStr.length;
		int id = Integer.parseInt(dataAsStr[0]);
		int i=1;
		for (; i < length-1; i++) {
			values.put(i-1, new Element(Integer.parseInt(dataAsStr[i])));
		}
		Element measure = new Element(Integer.parseInt(dataAsStr[i]));
		CubeGroup result = new CubeGroup(id,values,measure);
		return result;
	}
	
	
	@Override
	public void setup(Context context) {
		prob = Double.parseDouble(context.getConfiguration().get("sample.probability"));
		rand = new Random();
	}
	 
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		double x = rand.nextDouble();
		if (x<=prob) {
			byte[] data = CubeGroup.encode(retrieveKey(value.toString()), true, true);
			context.write(new BytesWritable(data), NullWritable.get());
		}
	}
	
	@Override
	public void cleanup(Context context) {
		rand = null;
	}
}
