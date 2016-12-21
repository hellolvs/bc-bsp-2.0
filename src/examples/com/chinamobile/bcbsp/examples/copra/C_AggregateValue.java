package com.chinamobile.bcbsp.examples.copra;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

public class C_AggregateValue extends AggregateValue<String,BSPMessage> {
	
	private String community_VertexNum;
	
	@Override
	public void initValue(Iterator<BSPMessage> messages, AggregationContextInterface context) {
		
		int maxOverLap = context.getJobConf().getConf().getInt("MaxOverlap", 0);

		BSPMessage message;
		String messageValue;
		String[] messV;
		long neighbors = 0;
		Map<String, Double> labelCoefficient = new HashMap<String, Double>();
		Map<String, Double> tmp_Labels = new HashMap<String, Double>();
		String[] lcs;
		String[] lc;
		String label;
		double coefficient;

		while (messages.hasNext()) {

			++neighbors;

			message = messages.next();
			messageValue = new String(message.getData());
			messV = messageValue.split("@");
			lcs = messV[1].split("\\|");

			for (int i = 0; i != lcs.length; ++i) {

				lc = lcs[i].split("-");
				label = lc[0];
				coefficient = Double.parseDouble(lc[1]);

				if (labelCoefficient.containsKey(label)) {
					labelCoefficient.put(label, labelCoefficient.get(label) + coefficient);
				} else {
					labelCoefficient.put(label, coefficient);
				}
			}
		}

		Set<String> randomLabels = new HashSet<String>(); 
		double maxCoefficient = 0;
		boolean findLabel = false;
		double tmp_coeffi;

		Iterator<String> it = labelCoefficient.keySet().iterator();
		while (it.hasNext()) {
			label = it.next();

			tmp_coeffi = labelCoefficient.get(label).doubleValue() / neighbors;

			if (tmp_coeffi > maxCoefficient) {
				maxCoefficient = tmp_coeffi;
				randomLabels.clear();
				randomLabels.add(label);
			} else if (tmp_coeffi == maxCoefficient) {
				randomLabels.add(label);
			}

			if (tmp_coeffi >= (1.0 / maxOverLap)) {
				tmp_Labels.put(label, tmp_coeffi);
				findLabel = true;
			}
		}
		if (!findLabel) {

			community_VertexNum = getMinLabel(randomLabels) + "-1|";

		} else {

			it = tmp_Labels.keySet().iterator();
			StringBuffer sb = new StringBuffer();
			while(it.hasNext()) {
				sb.append(it.next() + "-1|");
			}
			
			community_VertexNum = sb.toString();
			
		}
		
	}
	
	public String getMinLabel(Set<String> labels) {
		
		long minLabel = Integer.MAX_VALUE;
		String label;
		
		Iterator<String> it = labels.iterator();
		while(it.hasNext()) {
			label = it.next();
			if(minLabel > Long.parseLong(label)) {
				minLabel = Long.parseLong(label);
			}
		}
		
		return String.valueOf(minLabel);
		
	}
	
	@Override
	public void initValue(String s) {
		community_VertexNum = s;
	}

	@Override
	public void setValue(String s) {
		community_VertexNum = s;
	}

	@Override
	public String getValue() {
		return community_VertexNum;
	}

	@Override
	public String toString() {
		return community_VertexNum;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		community_VertexNum = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(community_VertexNum);
	}
}