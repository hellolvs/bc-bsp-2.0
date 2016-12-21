package com.chinamobile.bcbsp.examples.copra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.util.StaffAttemptID;

public class C_BSP extends BSP<BSPMessage> {

	private int superStep;

	private C_Vertex curr_V;
	private C_Edge curr_E;

	private BSPMessage message;
	private String messageValue;

	private int maxOverLap;
	private int updateAggValue = -1;

	private boolean shouldStop = false;
	private Map<String, Long> labelCount;

	private String aggValue;
	public static final String C_SUM = "aggregator.copra.sum";

	public static final Log LOG = LogFactory.getLog(C_BSP.class);
	public static int counter = 0;

	public static ArrayList<String> al = new ArrayList<String>();

	public void normalFormat(Map<String, Double> map) {

		Iterator<String> it = map.keySet().iterator();
		double sum = 0;

		while (it.hasNext()) {
			sum += map.get(it.next());
		}

		String label;
		it = map.keySet().iterator();

		while (it.hasNext()) {
			label = it.next();
			map.put(label, map.get(label).doubleValue() / sum);
		}

	}

	public String getVertexID(BSPMessage message) {
		String ms = new String(message.getData());
		return ms.split("@")[0];
	}

	public String getVertexValue(Map<String, Double> map) {
		StringBuffer sb = new StringBuffer();
		Iterator<String> it = map.keySet().iterator();
		String label;
		while (it.hasNext()) {
			label = it.next();
			sb.append(label + "-" + String.valueOf(map.get(label)) + "|");
		}
		return sb.toString();
	}
    //get the Min label
	/**public String getMinLabel(Set<String> labels) {

		long minLabel = Integer.MAX_VALUE;
		String label;

		Iterator<String> it = labels.iterator();
		while (it.hasNext()) {
			label = it.next();
			if (minLabel > Long.parseLong(label)) {
				minLabel = Long.parseLong(label);
			}
		}

		return String.valueOf(minLabel);

	}
	*/
	//get the Max label
	/**public String getMaxLabel(Set<String> labels) {

		long maxLabel = Integer.MIN_VALUE;
		String label;

		Iterator<String> it = labels.iterator();
		while (it.hasNext()) {
			label = it.next();
			if (maxLabel < Long.parseLong(label)) {
				maxLabel = Long.parseLong(label);
			}
		}

		return String.valueOf(maxLabel);

	}
    */
	//generate random label
	public String getRandomLabel(ArrayList<String> labels) {

		int i = (int) (Math.random() * labels.size());
		return labels.get(i);

	}
	
	private Map<String, Long> getLabelCount(String aggValue) {

		Map<String, Long> labelCount = new HashMap<String, Long>();

		String[] community_VertexNum = aggValue.split("\\|");
		String[] cvs;
		for (int i = 0; i != community_VertexNum.length; ++i) {
			cvs = community_VertexNum[i].split("-");
			if(cvs.length==2){
			if (labelCount.containsKey(cvs[0])) {
				labelCount.put(cvs[0], labelCount.get(cvs[0])
						+ Long.parseLong(cvs[1]));
			} else {
				labelCount.put(cvs[0], Long.parseLong(cvs[1]));
			}
			}else{
				LOG.info("gtt:"+community_VertexNum[i]);
			}
		}
		return labelCount;

	}

	private boolean remainLabels(Set<String> labels1, Set<String> labels2) {

		if (labels1.size() != labels2.size()) {
			return false;
		}

		Iterator<String> it = labels1.iterator();
		while (it.hasNext()) {
			if (!labels2.contains(it.next())) {
				return false;
			}
		}

		return true;
	}

	public boolean shouldStop(String aggValue) {

		Map<String, Long> map = getLabelCount(aggValue);

		if (labelCount == null) {
			labelCount = map;
			return false;
		}

		boolean remain = true;
		String label;
		Iterator<String> it;

		if (remainLabels(map.keySet(), labelCount.keySet())) {

			it = map.keySet().iterator();
			while (it.hasNext()) {
				label = it.next();

				if (map.get(label) < labelCount.get(label)) {
					labelCount.put(label, map.get(label));
					remain = false;
				}

			}

		} else {

			if (map.size() != labelCount.size()) {
				remain = false;
			} else {
				it = map.keySet().iterator();
				while (it.hasNext()) {
					label = it.next();
					if (!labelCount.containsKey(label)) {
						remain = false;
						break;
					}
					if (map.get(label) != labelCount.get(label)) {
						remain = false;
						break;
					}
				}
			}

			labelCount = map;
		}

		return remain;
	}

	@Override
	public void compute(Iterator<BSPMessage> messages,
			BSPStaffContextInterface context) throws Exception {

		String community_VertexNum;

		superStep = context.getCurrentSuperStepCounter();

		curr_V = (C_Vertex) context.getVertex();

		if (superStep == 0) {

			maxOverLap = context.getJobConf().getConf().getInt("MaxOverlap", 0);

			String labels = curr_V.getVertexValue();
			
			
			
			messageValue = curr_V.getVertexID() + "@" + labels;

			Iterator<C_Edge> it_E = curr_V.getAllEdges().iterator();
			while (it_E.hasNext()) {
				curr_E = it_E.next();
				message = new BSPMessage(curr_E.getVertexID(), messageValue
						.getBytes());
				LOG.info("gtt*****"+message);
				context.send(message);
			}

		} else {
			if (updateAggValue != superStep) {
				updateAggValue = superStep;
				//�ж�����
				if (context.getCurrentSuperStepCounter() != 1) {
				//if (context.getCurrentSuperStepCounter() >= 3){
					shouldStop = shouldStop(aggValue);
				}

			}

			if (shouldStop) {
				context.voltToHalt();
				return;
			}
//superstep !=1
			long neighbors = 0;

			Map<String, Double> labelCoefficient = new HashMap<String, Double>();
			Map<String, Double> labels = new HashMap<String, Double>();
			String[] lcs;
			String[] lc;
			String[] messV;
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
						labelCoefficient.put(label, labelCoefficient.get(label)
								+ coefficient);
					} else {
						labelCoefficient.put(label, coefficient);
					}

				}
			}

			//Set<String> randomLabels = new HashSet<String>();
			ArrayList randomLabels = new ArrayList<String>();
			double maxCoefficient = 0;
			boolean findLabel = false;
			double tmp_coeffi;

			Iterator<String> it = labelCoefficient.keySet().iterator();
			while (it.hasNext()) {
				label = it.next();

				tmp_coeffi = labelCoefficient.get(label).doubleValue()
						/ neighbors;

				if (tmp_coeffi > maxCoefficient) {
					maxCoefficient = tmp_coeffi;
					randomLabels.clear();
					randomLabels.add(label);
				} else if (tmp_coeffi == maxCoefficient) {
					randomLabels.add(label);
				}

				if (tmp_coeffi >= (1.0 / maxOverLap)) {
					labels.put(label, tmp_coeffi);
					findLabel = true;
				}
			}

			if (!findLabel) {

				//labels.put(getMaxLabel(randomLabels), maxCoefficient);
				labels.put(getRandomLabel(randomLabels), maxCoefficient);
				normalFormat(labels);
				
				
				curr_V.setVertexValue(getVertexValue(labels));
				//community_VertexNum = getMinLabel(randomLabels) + "-1|";
				//community_VertexNum = getMaxLabel(randomLabels) + "-1|";
				community_VertexNum = getRandomLabel(randomLabels) + "-1|";
			} else {
				normalFormat(labels);
				curr_V.setVertexValue(getVertexValue(labels));

				it = labels.keySet().iterator();
				StringBuffer sb = new StringBuffer();
				while (it.hasNext()) {
					sb.append(it.next() + "-1|");
				}

				community_VertexNum = sb.toString();

			}

			al.add(community_VertexNum);

			messageValue = curr_V.getVertexID() + "@" + curr_V.getVertexValue();

			Iterator<C_Edge> it_E = curr_V.getAllEdges().iterator();
			while (it_E.hasNext()) {
				curr_E = it_E.next();
				message = new BSPMessage(curr_E.getVertexID(), messageValue
						.getBytes());
				context.send(message);
			}
		}

	}

	@SuppressWarnings("deprecation")
	@Override
	public void initBeforeSuperStep(SuperStepContextInterface arg0) {
		if (arg0.getCurrentSuperStepCounter() == 0) {
			Configuration conf = new Configuration();
			try {
				FileSystem FS = FileSystem.get(conf);
				Path path = new Path("copra");
				FS.delete(path, true);
			} catch (IOException e) {

				e.printStackTrace();
			}
			
                return;
		}
		if (arg0.getCurrentSuperStepCounter() == 1) {
			return;
		}
		Configuration conf = new Configuration();
		FileSystem FS;
		al.clear();
		try {
			FS = FileSystem.get(conf);

			Path path = new Path("copra");

			FileStatus filelist[] = FS.listStatus(path);
			int size = filelist.length;

			for (int i = 0; i < size; i++) {
				String filename = filelist[i].getPath().getName();

				String spath = path.toString() + "/" + filename;
				Path pathe = new Path(spath);
				FileSystem fse = FileSystem.get(conf);
				FSDataInputStream fsi = fse.open(pathe);
				String line = fsi.readLine();
				String vn = "";
				while (line != null) {
					vn = vn + line;
					line = fsi.readLine();
				}

				al.add(vn);

			}
			aggValue = C_Aggregator.aggregate(al);
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	@Override
	public void initAfterSuperStep(StaffAttemptID sid) {
		String aggre = C_Aggregator.aggregate(al);
		try {
			publishaggre(aggre, sid);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void publishaggre(String aggre, StaffAttemptID sid)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem FS = FileSystem.get(conf);
		Path path = new Path("copra");
		if (!FS.exists(path)) {
			FS.mkdirs(path);
		}
		FSDataOutputStream out = create("copra/" + sid, conf);
		out.writeUTF(aggre);
		out.flush();
		out.close();
	}

	private FSDataOutputStream create(String path, Configuration conf)
			throws IOException {
		Path dstPath = new Path(path);
		FileSystem hdfs = dstPath.getFileSystem(conf);
		delete(path);
		return hdfs.create(dstPath);
	}

	private void delete(String path) throws IOException {
		Configuration conf = new Configuration();
		Path dstPath = new Path(path);
		FileSystem hdfs = dstPath.getFileSystem(conf);
		if (hdfs.exists(dstPath)) {
			hdfs.delete(dstPath, true);
		}
	}

}