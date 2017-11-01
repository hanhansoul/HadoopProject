package chapter5.example1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideJoiningReducer extends Reducer<Text, Text, Text, Text> {
	private List<Text> listA = new ArrayList<Text>();
	private List<Text> listB = new ArrayList<Text>();
	private String[] tempStringArr = null;
	String joinType = null;

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		joinType = context.getConfiguration().get("join.type");
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		listA.clear();
		listB.clear();
		for (Text text : values) {
			tempStringArr = text.toString().split("\t");
			if (tempStringArr[0].equals("A")) {
				listA.add(new Text(tempStringArr[1] + "\t" + tempStringArr[2]));
			} else if (tempStringArr[0].equals("B")) {
				listB.add(new Text(tempStringArr[1] + "\t" + tempStringArr[2]));
			}
		}
		executeJoining(context);
	}

	private void executeJoining(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (joinType.equalsIgnoreCase("inner")) {
			if (!listA.isEmpty() && !listB.isEmpty()) {
				for (Text ta : listA) {
					for (Text tb : listB) {
						context.write(ta, tb);
					}
				}
			}
		} else if (joinType.equalsIgnoreCase("leftouter")) {
			for (Text ta : listA) {
				if (!listB.isEmpty()) {
					for (Text tb : listB) {
						context.write(ta, tb);
					}
				} else {
					context.write(ta, new Text(""));
				}
			}
		} else if (joinType.equalsIgnoreCase("rightouter")) {
			for (Text tb : listB) {
				if (!listA.isEmpty()) {
					for (Text ta : listA) {
						context.write(ta, tb);
					}
				} else {
					context.write(new Text(""), tb);
				}
			}
		} else if (joinType.equalsIgnoreCase("fullouter")) {
			if (!listA.isEmpty()) {
				for (Text ta : listA) {
					if (!listB.isEmpty()) {
						for (Text tb : listB) {
							context.write(ta, tb);
						}
					} else {
						context.write(ta, new Text(""));
					}
				}
			} else {
				for (Text tb : listB) {
					context.write(new Text(""), tb);
				}
			}
		} else if (joinType.equalsIgnoreCase("anti")) {
			if (listA.isEmpty() ^ listB.isEmpty()) {
				for (Text ta : listA) {
					context.write(ta, new Text(""));
				}
				for (Text tb : listB) {
					context.write(new Text(""), tb);
				}
			}
		} else {
			throw new RuntimeException("Join type not set to inner, leftouter, rightouter, fullouter, or anti");
		}

	}
}
