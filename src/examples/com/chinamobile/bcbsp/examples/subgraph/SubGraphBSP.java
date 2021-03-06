package com.chinamobile.bcbsp.examples.subgraph;
/**
 * SubGraphBSP.java
 * @author moon
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
//import com.chinamobile.bcbsp.newversion.comm.IMessage;

/**
 * SubGraphBSP This is the user-defined arithmetic which implements {@link BSP}.
 * 
 * @author moon
 */
public class SubGraphBSP extends BSP<Sub_Message> {

	public static final Log LOG = LogFactory.getLog(SubGraphBSP.class);
	public static final String SUBGRAPH_INFO = "subgraph.info";
	public static final String MIN_SUPPORT = "min_support";
	public static final String VERTEX_NUM = "vertex_num";
	public static final String STOP_FLAG = "stop_flag";

	private int superStepCount;
	private int flag;
	private int vertex_num;
	private Sub_Message msg;
	private int min_support;
	private HashMap<String, List<String>> map = new HashMap<String, List<String>>();
	private int ifile;// 同一个顶点可能生成多个频繁子图，所以文件名应该不同，区分标识
	
	private int firstVertexid=-1;
	
//	private long messagesize=0;
//	private long maxmessagesize=0;
//	private int superstep=-1;

	public void print(HashMap<String, List<String>> arg) {
		LOG.info("[print]打印:" + arg + "中的内容：");
		Set<String> keySet = arg.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {

			String key = (String) it.next();
			LOG.info("[print]  key:" + key);

			List<String> l = arg.get(key);
			int ls = l.size();

			for (int i = 0; i < ls; i++) {
				LOG.info("[print]  key:" + key + " 对应的值 :" + l.get(i));
			}
		}
		LOG.info("[print]打印:" + arg + "中的内容完毕");
	}

	@Override
	public void compute(Iterator<Sub_Message> messages,
			BSPStaffContextInterface context) throws Exception {
			//LOG.info("sjz test in compute function");
		// jobconf = context.getJobConf();
		superStepCount = context.getCurrentSuperStepCounter();
		
		SMVertex thisVertex = (SMVertex) context.getVertex();
		Iterator<Edge> outgoingEdges = context.getOutgoingEdges();
//		LOG.info("\n"
//				+ "the current superstep is :"+superStepCount+"\n the current vertex is"+thisVertex.vertexID+"\n the current"
//				+ "vertex edge Num is: "+thisVertex.getEdgesNum());
		// LOG.info(" 顶点：" + thisVertex.vertexID);
		if (superStepCount == 0) {
			// HashMap<String, List<String>> mapOfLocal = new HashMap<String,
			// List<String>>();
			//
			// Set<String> keySet = map.keySet();
			// Iterator it = keySet.iterator();
			// while (it.hasNext()) {
			// String key = (String) it.next();
			// List<String> value = null;
			// int smallVID = Integer.valueOf(key.charAt(0));
			// if (thisVertex.vertexID == smallVID) {
			// value = map.get(key);
			// mapOfLocal.put(key, value);
			// }
			// }
            
			while (outgoingEdges.hasNext()) {
				SMEdge edge = (SMEdge) outgoingEdges.next();
				// LOG.info(" 顶点的边：" + edge.intoString());

				String key = null;
				String value = null;
				String edgeInfo[] = edge.intoString().split(
						Constantself.SPLIT_FLAGD);
				// 构造规范编码key
				if (thisVertex.vertexID < edge.vertexID) {
					if (thisVertex.vertexValue < edge.vertexValue) {
						key = new String(edgeInfo[1] + edgeInfo[2]
								+ thisVertex.vertexValue);
					} else {
						key = new String(thisVertex.vertexValue + edgeInfo[2]
								+ edgeInfo[1]);
					}
					// LOG.info(" 规范编码：" + key);
					/* genggai */
					value = new String(thisVertex.intoStrSelf()
							+ Constantself.SPLIT_FLAGD + edgeInfo[0]
							+ Constantself.SPLIT_FLAGD + edgeInfo[1]);
					// LOG.info(" 规范编码key对应的value：" + value);
					 HashMD5<String> hm=new HashMD5<String>(this.vertex_num);
					 int hashKey=hm.getPartitionID(key);//采用MD5哈希方法
//					int hashKey = hash(key) % this.vertex_num;
				//	LOG.info(" hashkey：  " + hashKey);
					String messageValue = new String(key
							+ Constantself.KV_SPLIT_FLAG + value);
					//LOG.info("superstep :  "+superStepCount+  "      messageValue：  " + messageValue);
					//songjianze modified  2 lines
					//msg = ( Sub_Message)context.getMessage();//??????????
					
					msg = new Sub_Message();
					msg.setContent(messageValue );
					msg.setMessageId(hashKey);
//					if(this.superstep!=this.superStepCount){
//						this.superstep=this.superStepCount;
//						messagesize=0;
//					}
						
//					messagesize+=msg.size();
//					if(msg.size()>maxmessagesize)
//						maxmessagesize=msg.size();
				//	LOG.info("superstep :  "+superStepCount+ "send message destidation iD : message"+msg.getMessageId()+"   :  "+msg.getContent());
					context.send(msg);
				//	LOG.info("superstep :  "+superStepCount+ "send message destidation iD : message"+msg.getMessageId()+"   :  "+msg.getContent());
					/* genggai */

				}
			}//while
//			LOG.info("current superstep "+ superStepCount+" messagesize is "+messagesize+"the max message size is:"+maxmessagesize);
			// context.updateVertex(thisVertex);
		} else if (superStepCount == 1) {
			//added by songjianze
//			if(this.superstep!=this.superStepCount){
//				this.superstep=this.superStepCount;
//				messagesize=0;
//			}
			if(firstVertexid==-1)
				this.firstVertexid=(Integer) context.getVertex().getVertexID();
			
			
//			end of add
			String messageValue = null;
			map.clear();
			int mcount=0;//for test
			while (messages.hasNext()) {
				mcount++;
				//note modified
				messageValue = ((Sub_Message)(messages.next())).getContent();
				//LOG.info("superstep :  "+superStepCount+ "    get one message     "+messageValue );
				//LOG.info("messageid is : "+((Sub_Message)(messages.next())).getMessageId());
				String msg[] = messageValue.split(Constantself.KV_SPLIT_FLAG);
				addElementOfMap(msg[0], msg[1]);
			}
//		 LOG.info(" 超级步 " + superStepCount + ":"+" message count :"+ mcount);
//		 print(map);
			Set<String> keySet = map.keySet();
			for (Iterator it1 = keySet.iterator(); it1.hasNext();) {

				String key = (String) it1.next();
			//	LOG.info("key is : "+key);
				List<String> list = map.get(key);
				int sum = list.size();
//				LOG.info("sum is : "+sum);
				/*
				 * int sum=0; for (String val : list) { String num[] =
				 * val.toString().split(Constantself.SPLIT_FLAG); sum +=
				 * Integer.valueOf(num[1]); }
				 */
				if (sum >= min_support) {
					//LOG.info("there is frequent model");
					/** *此步骤需要将得到的规范编码传入到图类中，图类以此构造链表形式的图，存入频繁-1边集中** */
					Configuration conf = new Configuration();
					FileSystem hdfs = FileSystem.get(conf);

					Path hdfsFile = new Path("bspoutput/subgraph"
							+ superStepCount +"/"+ this.firstVertexid+"#"+(ifile++)
							+ "#"+thisVertex.getVertexID() + ".txt");

					try {

						FSDataOutputStream out = hdfs.create(hdfsFile);
						out
								.writeBytes("***********sub_graph 1***********************"
										+ "\n");
						out.writeBytes("\n");
						out.writeBytes("V" + Constantself.KV_SPLIT_FLAG
								+ String.valueOf(0)
								+ Constantself.KV_SPLIT_FLAG
								+ key.toString().charAt(0) + "\n");
						out.writeBytes("\n");
						out.writeBytes("V" + Constantself.KV_SPLIT_FLAG
								+ String.valueOf(1)
								+ Constantself.KV_SPLIT_FLAG
								+ key.toString().charAt(2) + "\n");
						out.writeBytes("\n");
						out.writeBytes("E" + Constantself.KV_SPLIT_FLAG
								+ String.valueOf(0)
								+ Constantself.KV_SPLIT_FLAG
								+ String.valueOf(1)
								+ Constantself.KV_SPLIT_FLAG
								+ key.toString().charAt(1) + "\n");
						out.writeBytes("\n");
						out.writeBytes("***********sub_graph 1***********************");
						out.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
					// LOG.info("***********sub_graph
					// 1***********************");
					// /* 顶点信息写入 */
					// LOG.info("V" + Constantself.KV_SPLIT_FLAG
					// + String.valueOf(0) + Constantself.KV_SPLIT_FLAG
					// + key.toString().charAt(0));
					// LOG.info("V" + Constantself.KV_SPLIT_FLAG
					// + String.valueOf(1) + Constantself.KV_SPLIT_FLAG
					// + key.toString().charAt(2));
					// /* 边信息写入 */
					// LOG.info("E" + Constantself.KV_SPLIT_FLAG
					// + String.valueOf(0) + Constantself.KV_SPLIT_FLAG
					// + String.valueOf(1) + Constantself.KV_SPLIT_FLAG
					// + key.toString().charAt(1));
					/** *图类以此构造链表形式的图，存入频繁-1边集完毕** */

					String outkey = null;
					String outvalue = null;
					for (int j = 0; j < list.size(); j++) {
						String val = list.get(j);
						// val 0,a,1,b
						String splitkey[] = val.split(Constantself.SPLIT_FLAGD);
						outkey = splitkey[0];
						outvalue = val + Constantself.SPLIT_FLAGD
								+ key.toString().charAt(1)
								+ Constantself.SPLIT_FLAG + key.toString();
						// LOG.info("超级步" + superStepCount + ":" + "得到的key:value " + outkey + "\t" + outvalue);
						messageValue = new String(outvalue);
						//note modified
						msg = ( Sub_Message)context.getMessage();//??????????
						msg.setContent(messageValue );
						msg.setMessageId(Integer.parseInt(outkey));
						context.send(msg);
//						added by songjianze
//						messagesize+=msg.size();
//						if(msg.size()>maxmessagesize)
//							maxmessagesize=msg.size();
//						------end of add
						outkey = splitkey[2];
						outvalue = val + Constantself.SPLIT_FLAGD
								+ key.toString().charAt(1)
								+ Constantself.SPLIT_FLAG + key.toString();
						// LOG.info("超级步" + superStepCount + ":" + "得到的key:value " + outkey + "\t" + outvalue);
						messageValue = new String(outvalue);
						msg = ( Sub_Message)context.getMessage();//??????????
						msg.setContent(messageValue );
						msg.setMessageId(Integer.parseInt(outkey));
						//LOG.info("superstep 1 massage is:  "+messageValue);
						context.send(msg);
//						added by songjianze
//						messagesize+=msg.size();
//						if(msg.size()>maxmessagesize)
//							maxmessagesize=msg.size();
//						---end of add
						// 写HDFS
					}
				}
			}
//			LOG.info("current superstep "+ superStepCount+" messagesize is "+messagesize+"the max message size is:"+maxmessagesize);
		}
		else if (superStepCount % 2 == 1) {// 求最大团
//			LOG.info("[sjz test]"+"superStepCount % 2 == 1)  求最大团");
			
			//added by songjianze
//			if(this.superstep!=this.superStepCount){
//				this.superstep=this.superStepCount;
//				messagesize=0;
//			}
//			end of add
			String messageValue = null;
			int mcount=0;//for test
			map.clear();
			while (messages.hasNext()) {
				mcount++;
				//note modified
				messageValue = ((Sub_Message)(messages.next())).getContent();
				String msg[] = messageValue.split(Constantself.KV_SPLIT_FLAG);
				addElementOfMap(msg[0], msg[1]);
			}
			/*if(mcount>0){
			 LOG.info("  max click  超级步    " + superStepCount + ":" +"  messages count   "+mcount );
			 print(map);
			}*/
			Set<String> keySet = map.keySet();
			for (Iterator it1 = keySet.iterator(); it1.hasNext();) {
				String key = (String) it1.next();

				List<String> values = map.get(key);

				String outkey = null;
				String outvalue = null;
				ArrayList<Set<Pair>> edgeset = new ArrayList<Set<Pair>>();
				Set<String> normalcode = new TreeSet<String>();

				for (String val : values) {
					String num[] = val.toString().split(
							Constantself.SEPA_SPLIT_FLAG);
					String splitnum0[] = num[0].split(Constantself.SPLIT_FLAG);
					String splitnum1[] = num[1].split(Constantself.SPLIT_FLAG);
					normalcode.add(splitnum0[1]);
					normalcode.add(splitnum1[1]);
					Set<Pair> embedset = new TreeSet<Pair>();
					if (num[0].contains(Constantself.AND_SPLIT_FLAG)) {
						String getIpair0[] = splitnum0[0]
								.split(Constantself.AND_SPLIT_FLAG);
						String getIpair1[] = splitnum1[0]
								.split(Constantself.AND_SPLIT_FLAG);
						for (int i = 0; i < getIpair0.length; i++) {
							// Pair p0 = new Pair(getIpair0[i].charAt(0),
							// getIpair0[i].charAt(2));
							// embedset.add(p0);
							// Pair p1 = new Pair(getIpair1[i].charAt(0),
							// getIpair1[i].charAt(2));
							// embedset.add(p1);//修改，这样得到的顶点ID是错误的
							String Ipair0[] = getIpair0[i]
									.split(Constantself.SPLIT_FLAGD);
							Pair p0 = new Pair(Integer.parseInt(Ipair0[0]),
									Integer.parseInt(Ipair0[2]));// 抽取出顶点id,
							embedset.add(p0);
							// Pair p1 = new Pair(getIpair1[i].charAt(0),
							// getIpair1[i]
							// .charAt(2));
							String Ipair1[] = getIpair1[i]
									.split(Constantself.SPLIT_FLAGD);
							Pair p1 = new Pair(Integer.parseInt(Ipair1[0]),
									Integer.parseInt(Ipair1[2]));// 抽取出顶点id,
							embedset.add(p1);

						}
					} else {
						String Ipair0[] = splitnum0[0]
								.split(Constantself.SPLIT_FLAGD);
						Pair p0 = new Pair(Integer.parseInt(Ipair0[0]), Integer
								.parseInt(Ipair0[2]));// 抽取出顶点id,
						embedset.add(p0);
						String Ipair1[] = splitnum1[0]
								.split(Constantself.SPLIT_FLAGD);
						Pair p1 = new Pair(Integer.parseInt(Ipair1[0]), Integer
								.parseInt(Ipair1[2]));// 抽取出顶点id,
						embedset.add(p1);
					}
					if (!edgeset.contains(embedset))// 一个频繁子图可能由多对图连接得到，为了去重
						edgeset.add(embedset);
				}
				if (edgeset.size() >= min_support) {
//					LOG.info("[sjz test] find frequent model:"+(superStepCount + 1) / 2);
					
					
					//changed by songjianze	
//					Graph_Click click = new Graph_Click();
//					boolean count = click.new InstanceGraph(edgeset,min_support).CountMIS();
					Graph_Click click = Graph_Click.getInstance();
					boolean count = click.getInstanceGraph(edgeset, min_support).CountMIS();
					//added by songjianze
					
//					boolean count=true;
					if (count) {
						Configuration conf = new Configuration();
						FileSystem hdfs = FileSystem.get(conf);

						Path hdfsFile = new Path("bspoutput/subgraph"
								+ (superStepCount + 1) / 2+"/"+this.firstVertexid+"#"+(ifile++)
								+ "#"+thisVertex.getVertexID() + ".txt");

						try {

							FSDataOutputStream out = hdfs.create(hdfsFile);
//							SCGraph graph = new SCGraph(key.toString());
							/*
							 * changed by songjianze
							 */
							SCGraph graph = SCGraph.getinstance(key.toString());
							/*
							 * changed by songjianze
							 */
							out.writeBytes("***********sub_graph"
									+ (superStepCount + 1) / 2
									+ "***********************" + "\n");
							out.writeBytes("\n");
							for (int i = 0; i < graph.vertexsList.size(); i++) {
								SCVertex vi = graph.vertexsList.get(i);
								out.writeBytes("V" + Constantself.KV_SPLIT_FLAG
										+ vi.vertexID
										+ Constantself.KV_SPLIT_FLAG
										+ vi.vertexValue + "\n");
								out.writeBytes("\n");
							}

							for (int i = 0; i < graph.vertexsList.size(); i++) {
								SCVertex Vi = graph.vertexsList.get(i);
								List<SCEdge> edgelist = Vi.edgesList;
								for (int j = 0; j < edgelist.size(); j++) {
									SCEdge e = edgelist.get(j);
									if (!e.flag) {
										SCVertex Vj = graph
												.getVertexByID(e.vertexID);
										Vj.markEdgeByVID(Vi.vertexID);
										out.writeBytes("E"
												+ Constantself.KV_SPLIT_FLAG
												+ Vi.vertexID
												+ Constantself.KV_SPLIT_FLAG
												+ Vj.vertexID
												+ Constantself.KV_SPLIT_FLAG
												+ e.edgeValue + "\n");
										out.writeBytes("\n");

									}
								}
							}
							out.writeBytes("***********sub_graph"
									+ (superStepCount + 1) / 2
									+ "***********************");
							out.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
						/** 将频繁子图打印日志* */
						// SCGraph graph = new SCGraph(key.toString());
						// LOG.info("***********sub_graph" + (superStepCount +
						// 1)
						// / 2 + "***********************");
						// for (int i = 0; i < graph.vertexsList.size(); i++) {
						// SCVertex vi = graph.vertexsList.get(i);
						// LOG.info("V" + Constantself.KV_SPLIT_FLAG
						// + vi.vertexID + Constantself.KV_SPLIT_FLAG
						// + vi.vertexValue);
						// }
						//
						// for (int i = 0; i < graph.vertexsList.size(); i++) {
						// SCVertex Vi = graph.vertexsList.get(i);
						// List<SCEdge> edgelist = Vi.edgesList;
						// for (int j = 0; j < edgelist.size(); j++) {
						// SCEdge e = edgelist.get(j);
						// if (!e.flag) {
						// SCVertex Vj = graph
						// .getVertexByID(e.vertexID);
						// Vj.markEdgeByVID(Vi.vertexID);
						// LOG.info("E" + Constantself.KV_SPLIT_FLAG
						// + Vi.vertexID
						// + Constantself.KV_SPLIT_FLAG
						// + Vj.vertexID
						// + Constantself.KV_SPLIT_FLAG
						// + e.edgeValue);
						//
						// }
						// }
						// }
						/** 将频繁子图打印日志* */

						int min_count = 0;
						Iterator<String> it = normalcode.iterator();
						String mincode1 = null;
						String mincode2 = null;
						while (min_count < 2 && it.hasNext()) {
							String str = it.next();
							if (mincode1 == null) {
								mincode1 = str;
								min_count++;
							} else if (mincode2 == null) {
								mincode2 = str;
								min_count++;
							}
						}

						Map<String, List<String>> outbuffer = new HashMap<String, List<String>>();
						for (int i = 0; i < values.size(); i++) {
							String val = values.get(i);
							String edge[] = val.toString().split(
									Constantself.SEPA_SPLIT_FLAG);
							String splitedge0[] = edge[0]
									.split(Constantself.SPLIT_FLAG);
							String splitedge1[] = edge[1]
									.split(Constantself.SPLIT_FLAG);

							if ((splitedge0[1].equals(mincode1))
									|| (splitedge0[1].equals(mincode2))) {
								String nextsplitedge0[] = splitedge0[0]
										.split(Constantself.AND_SPLIT_FLAG);
								String edgeunion = CommonUtils.union(
										splitedge0[0], splitedge1[0]);
								StringBuffer tempIdpairs = new StringBuffer();
								for (int j = 0; j < nextsplitedge0.length; j++) {
									String e;
									if (j < nextsplitedge0.length - 1) {
										// e = new
										// String(nextsplitedge0[j].charAt(0)
										// + Constantself.SPLIT_FLAGD
										// + nextsplitedge0[j].charAt(4)
										// + Constantself.SPLIT_FLAG);
										String Ipair1[] = nextsplitedge0[j]
												.split(Constantself.SPLIT_FLAGD);
										e = new String(Ipair1[0]
												+ Constantself.SPLIT_FLAGD
												+ Ipair1[2]
												+ Constantself.SPLIT_FLAG);

									} else {
										// e = new
										// String(nextsplitedge0[j].charAt(0)
										// + Constantself.SPLIT_FLAGD
										// + nextsplitedge0[j].charAt(4));
										String Ipair1[] = nextsplitedge0[j]
												.split(Constantself.SPLIT_FLAGD);
										e = new String(Ipair1[0]
												+ Constantself.SPLIT_FLAGD
												+ Ipair1[2]);
									}
									tempIdpairs.append(e.toString());
								}

								List<String> l;
								if (outbuffer.containsKey(tempIdpairs
										.toString())) {
									l = (List<String>) outbuffer
											.get(tempIdpairs.toString());
									boolean f = false;
									for (int ii = 0; ii < l.size(); ii++) {
										if (l.get(ii).equals(
												edgeunion.toString())) {
											f = true;
											break;
										}
									}
									if (!f) {
										l.add(edgeunion);
										outbuffer
												.put(tempIdpairs.toString(), l);
										outkey = tempIdpairs.toString();
										outvalue = edgeunion
												+ Constantself.SPLIT_FLAG
												+ key.toString();
										 HashMD5<String> hm=new HashMD5<String>(this.vertex_num);
										 int hashkey=hm.getPartitionID(outkey);//采用MD5哈希方法
//										int hashkey = hash(outkey)
//												% this.vertex_num;// 采用自己定义的哈希方法
//										 LOG.info("hashkey = :" + hashkey);
//										 LOG.info("超级步" + superStepCount + ":"
//										 + "send的key:value " + outkey
//										 + "\t" + outvalue);
										 
										messageValue = new String(outkey
												+ Constantself.KV_SPLIT_FLAG
												+ outvalue);
										msg = ( Sub_Message)context.getMessage();//??????????
										msg.setContent(messageValue );
										msg.setMessageId(hashkey);
										context.send(msg);
										
//										added by songjianze
//										messagesize+=msg.size();
//										if(msg.size()>maxmessagesize)
//											maxmessagesize=msg.size();
//										---end of add
										
										// int smallVID = CommonUtils
										// .getSmallVID(outkey);
										// LOG.info("超级步" + superStepCount + ":"
										// + "得到的key:value " + outkey + "\t"
										// + outvalue);
										// messageValue = new String(outkey
										// + Constantself.KV_SPLIT_FLAG
										// + outvalue);
										// msg = new BSPMessage(String
										// .valueOf(smallVID), messageValue
										// .getBytes());
									

									}
								} else {
									l = new ArrayList<String>();
									l.add(edgeunion);
									outbuffer.put(tempIdpairs.toString(), l);
									outkey = tempIdpairs.toString();
									outvalue = edgeunion
											+ Constantself.SPLIT_FLAG
											+ key.toString();
									 HashMD5<String> hm=new
									 HashMD5<String>(this.vertex_num);
									 int hashkey=hm.getPartitionID(outkey);//采用MD5哈希方法
//									int hashkey = hash(outkey)
//											% this.vertex_num;// 采用自定义哈希
//									 LOG.info("hashkey = :" + hashkey);
//									 LOG.info("超级步" + superStepCount + ":"
//									 + "send的key:value " + outkey + "\t"
//									 + outvalue);
									 
									messageValue = new String(outkey
											+ Constantself.KV_SPLIT_FLAG
											+ outvalue);
									msg = ( Sub_Message)context.getMessage();//??????????
									msg.setContent(messageValue );
									msg.setMessageId(hashkey);
									context.send(msg);
									
//									added by songjianze
//									messagesize+=msg.size();
//									if(msg.size()>maxmessagesize)
//										maxmessagesize=msg.size();
//									---end of add
									
									// int smallVID =
									// CommonUtils.getSmallVID(outkey);
									// LOG.info("超级步" + superStepCount + ":"
									// + "得到的key:value " + outkey + "\t"
									// + outvalue);
									// messageValue = new String(outkey
									// + Constantself.KV_SPLIT_FLAG + outvalue);
									// msg = new
									// BSPMessage(String.valueOf(smallVID),
									// messageValue.getBytes());
									
								}
							}
							if ((splitedge1[1].equals(mincode1))
									|| (splitedge1[1].equals(mincode2))) {
								String nextsplitedge1[] = splitedge1[0]
										.split(Constantself.AND_SPLIT_FLAG);
								String edgeunion = CommonUtils.union(
										splitedge0[0], splitedge1[0]);
								StringBuffer tempIdpairs = new StringBuffer();
								for (int j = 0; j < nextsplitedge1.length; j++) {
									String t;
									if (j < nextsplitedge1.length - 1) {
										String Ipair1[] = nextsplitedge1[j]
												.split(Constantself.SPLIT_FLAGD);
										t = new String(Ipair1[0]
												+ Constantself.SPLIT_FLAGD
												+ Ipair1[2]
												+ Constantself.SPLIT_FLAG);
									} else {
										String Ipair1[] = nextsplitedge1[j]
												.split(Constantself.SPLIT_FLAGD);
										t = new String(Ipair1[0]
												+ Constantself.SPLIT_FLAGD
												+ Ipair1[2]);
									}
									tempIdpairs.append(t.toString());
								}
								List<String> l;
								if (outbuffer.containsKey(tempIdpairs
										.toString())) {
									l = (List<String>) outbuffer
											.get(tempIdpairs.toString());
									boolean f = false;
									for (int ii = 0; ii < l.size(); ii++) {
										if (l.get(ii).equals(
												edgeunion.toString())) {
											f = true;
											break;
										}
									}
									if (!f) {
										l.add(edgeunion);
										outbuffer
												.put(tempIdpairs.toString(), l);
										outkey = tempIdpairs.toString();
										outvalue = edgeunion
												+ Constantself.SPLIT_FLAG
												+ key.toString();
										 HashMD5<String> hm=new HashMD5<String>(this.vertex_num);
										 int hashkey=hm.getPartitionID(outkey);//采用MD5哈希方法
//										int hashkey = hash(outkey)
//												% this.vertex_num;
//										 LOG.info("hashkey = :" + hashkey);
//										 LOG.info("超级步" + superStepCount + ":"
//										 + "send的key:value " + outkey
//										 + "\t" + outvalue);
										messageValue = new String(outkey
												+ Constantself.KV_SPLIT_FLAG
												+ outvalue);
										msg = ( Sub_Message)context.getMessage();//??????????
										msg.setContent(messageValue );
										msg.setMessageId(hashkey);
										context.send(msg);
//										added by songjianze
//										messagesize+=msg.size();
//										if(msg.size()>maxmessagesize)
//											maxmessagesize=msg.size();
//										---end of add
										
										// int smallVID = CommonUtils
										// .getSmallVID(outkey);
										// LOG.info("超级步" + superStepCount + ":"
										// + "得到的key:value " + outkey
										// + "\t" + outvalue);
										// messageValue = new String(outkey
										// + Constantself.KV_SPLIT_FLAG
										// + outvalue);
										// msg = new BSPMessage(String
										// .valueOf(smallVID), messageValue
										// .getBytes());
										
									}
								} else {
									l = new ArrayList<String>();
									l.add(edgeunion);
									outbuffer.put(tempIdpairs.toString(), l);
									outkey = tempIdpairs.toString();
									outvalue = edgeunion
											+ Constantself.SPLIT_FLAG
											+ key.toString();
									// int smallVID =
									// CommonUtils.getSmallVID(outkey);
									 HashMD5<String> hm=new HashMD5<String>(this.vertex_num);
									 int hashkey=hm.getPartitionID(outkey);//采用MD5哈希方法
//									int hashkey = hash(outkey)
//											% this.vertex_num;
//									 LOG.info("hashkey = :" + hashkey);
//									 LOG.info("超级步" + superStepCount + ":"
//									 + "send的key:value " + outkey + "\t"
//									 + outvalue);
									messageValue = new String(outkey
											+ Constantself.KV_SPLIT_FLAG
											+ outvalue);
									msg = ( Sub_Message)context.getMessage();//??????????
									msg.setContent(messageValue );
									msg.setMessageId(hashkey);
									context.send(msg);
									
//									added by songjianze
//									messagesize+=msg.size();
//									if(msg.size()>maxmessagesize)
//										maxmessagesize=msg.size();
//									---end of add
									
									// msg = new
									// BSPMessage(String.valueOf(smallVID),
									// messageValue.getBytes());
									
								}
							}
						}
					}
				}

			}// end if(out)
//			LOG.info("current superstep "+ superStepCount+" messagesize is "+messagesize+"the max message size is:"+maxmessagesize);
		} else if (superStepCount % 2 == 0) {// 根据上一步发来的消息求候选子图
//			LOG.info("[sjz test]"+"superStepCount % 2 == 0 根据上一步发来的消息求候选子图");
			
			//added by songjianze
//			if(this.superstep!=this.superStepCount){
//				this.superstep=this.superStepCount;
//				messagesize=0;
//			}
//			end of add
			
			String messageValue = null;
			if (superStepCount == 2) {
				List<String> l = new ArrayList<String>();
				while (messages.hasNext()) {
					messageValue = ((Sub_Message)(messages.next())).getContent();
					l.add(messageValue);
				}
//				if(l.size()>0)
//				 LOG.info("打印出list中的内容");
//				if(l.size()>0)
//				LOG.info("[sjz test]vertex id is: "+ thisVertex.vertexID);
//				 for (int i = 0; i < l.size(); i++) {
//				 LOG.info(l.get(i));
//				 }
//				 if(l.size()>0)
//				 LOG.info("打印完list中的内容。");
				for (int i = 0; i < l.size() - 1; i++) {
					String embed1 = l.get(i);
					String outkey = null;
					String outvalue = null;
					for (int j = i + 1; j < l.size(); j++) {
//						LOG.info("in superstep 2 for loop ");
						String embed2 = l.get(j);
						outvalue = embed1.toString()
								+ Constantself.SEPA_SPLIT_FLAG
								+ embed2.toString();
						String splitembed1[] = embed1.toString().split(
								Constantself.SPLIT_FLAG);
						String splitembed2[] = embed2.toString().split(
								Constantself.SPLIT_FLAG);
						TreeSet<String> edgeunion = new TreeSet<String>();
						edgeunion.add(splitembed1[0]);
						edgeunion.add(splitembed2[0]);
//						Iterator it = edgeunion.iterator();
//						while (it.hasNext()) {
//							System.out.println(it.next());
//						}
						
						
//						SCGraph graph = new SCGraph(edgeunion);
//						StandardCode sc = new StandardCode();
//						outkey = sc.getstandardCode(graph);
						/*
						 * changed by songjianze
						 */
						SCGraph graph = SCGraph.getinstance(edgeunion);
						outkey = StandardCode.getstandardCode(graph);
						/*
						 * changed by songjianze
						 */
						// int hashKey = outkey.hash() % this.vertex_num;
						 HashMD5<String> hm=new
						 HashMD5<String>(this.vertex_num);
						 int hashkey=hm.getPartitionID(outkey);//采用MD5哈希方法
//						int hashKey = hash(outkey) % this.vertex_num;
//						 LOG.info(" hashkey： " + hashkey);
						messageValue = new String(outkey
								+ Constantself.KV_SPLIT_FLAG + outvalue);
						
						 LOG.info(" [sendmessage] id: " + hashkey + " meassagevalue： " + messageValue);
						msg = ( Sub_Message)context.getMessage();//??????????
						msg.setContent(messageValue );
						msg.setMessageId(hashkey);
						context.send(msg);
						
//						added by songjianze
//						messagesize+=msg.size();
//						if(msg.size()>maxmessagesize)
//							maxmessagesize=msg.size();
//						---end of add
						
					}
				}
			} else {
				if (flag == 0) {
					context.voltToHalt();
					return;
				}
				int mcount=0;
				map.clear();
				while (messages.hasNext()) {
					mcount++;
					messageValue = ((Sub_Message)(messages.next())).getContent();
					String msg[] = messageValue
							.split(Constantself.KV_SPLIT_FLAG);
					addElementOfMap(msg[0], msg[1]);
				}
//				 LOG.info("     超级步       " + superStepCount + ":"+"   message count  "+mcount);
//				 print(map);
				Set<String> keySet = map.keySet();
				Iterator it = keySet.iterator();
				while (it.hasNext()) {
					String key = (String) it.next();
					List<String> list = null;
					list = map.get(key);
					for (int i = 0; i < list.size() - 1; i++) {
						String embed1 = list.get(i);
						String outkey = null;
						String outvalue = null;
						for (int j = i + 1; j < list.size(); j++) {
							String embed2 = list.get(j);
							outvalue = embed1.toString()
									+ Constantself.SEPA_SPLIT_FLAG
									+ embed2.toString();
							String splitembed1[] = embed1.toString().split(
									Constantself.SPLIT_FLAG);
							String splitembed2[] = embed2.toString().split(
									Constantself.SPLIT_FLAG);
							String edgeset1[] = splitembed1[0]
									.split(Constantself.AND_SPLIT_FLAG);
							String edgeset2[] = splitembed2[0]
									.split(Constantself.AND_SPLIT_FLAG);
							TreeSet<String> edgeunion = new TreeSet<String>();
							for (int m = 0; m < edgeset1.length; m++) {
								edgeunion.add(edgeset1[m]);
							}
							for (int n = 0; n < edgeset2.length; n++) {
								edgeunion.add(edgeset2[n]);
							}

//							SCGraph graph = new SCGraph(edgeunion);
//							StandardCode sc = new StandardCode();
//							outkey = sc.getstandardCode(graph);
							
							/*
							 * changed by songjianze
							 */
							SCGraph graph = SCGraph.getinstance(edgeunion);
							outkey = StandardCode.getstandardCode(graph);
							/*
							 * changed by songjianze
							 */
							
//							 LOG.info("超级步" + superStepCount + ":"
//							 + "send的key:value" + outkey + "\t"
//							 + outvalue);
//							 int hashKey = outkey.hash() % this.vertex_num;
							 HashMD5<String> hm=new HashMD5<String>(this.vertex_num);
							 int hashkey=hm.getPartitionID(outkey);//采用MD5哈希方法
							// LOG.info("destination ID is: "+hashkey);
//							int hashKey = hash(outkey) % this.vertex_num;
//							 LOG.info(" hashkey： " + hashKey);
							messageValue = new String(outkey
									+ Constantself.KV_SPLIT_FLAG + outvalue);
							msg = ( Sub_Message)context.getMessage();//??????????
							msg.setContent(messageValue );
							msg.setMessageId(hashkey);
							context.send(msg);
							
//							added by songjianze
//							messagesize+=msg.size();
//							if(msg.size()>maxmessagesize)
//								maxmessagesize=msg.size();
//							---end of add
							
						}// end for(inner)
					}// end for(middle)

				}// end while
			}
//			LOG.info("current superstep "+ superStepCount+" messagesize is "+messagesize+"the max message size is:"+maxmessagesize);
		}
//				LOG.info("----------sjz test "+"the current vertex is"+thisVertex.vertexID+"compute over-------------");
		//LOG.info("sjz test end of compute");
	}// end-compute

	private void setInitialMap(HashMap<String, List<String>> contents) {
		Set<String> keySet = contents.keySet();
		for (Iterator it1 = keySet.iterator(); it1.hasNext();) {

			String key = (String) it1.next();

			List<String> list = contents.get(key);
			int sum = list.size();
			/*
			 * int sum=0; for (String val : list) { String num[] =
			 * val.toString().split(Constantself.SPLIT_FLAG); sum +=
			 * Integer.valueOf(num[1]); }
			 */
			if (sum >= min_support) {
				if (flag == 0)
					flag = 1;// 迭代标识
				// addElementOfMap(flag, null);
				String outkey = null;
				String outvalue = null;
				for (int j = 0; j < list.size(); j++) {
					String val = list.get(j);
					String num[] = val.toString()
							.split(Constantself.SPLIT_FLAG);
					String splitkey[] = num[0].split(Constantself.SPLIT_FLAGD);
					outkey = splitkey[0];
					outvalue = num[0] + Constantself.SPLIT_FLAGD
							+ key.toString().charAt(1)
							+ Constantself.SPLIT_FLAG + key.toString();
					addElementOfMap(outkey, outvalue);

					outkey = splitkey[2];
					outvalue = num[0] + Constantself.SPLIT_FLAGD
							+ key.toString().charAt(1)
							+ Constantself.SPLIT_FLAG + key.toString();
					addElementOfMap(outkey, outvalue);
					// 写HDFS
				}
			}
		}
	}

	private int hash(String arg) {
		int sum = 0;
		for (int i = 0; i < arg.length(); i++) {
			sum += arg.charAt(i) - '0';
		}
		return sum;
	}

	private void addElementOfMap(String key, String edgeInfo) {
		// List<String> l = initialResult.get(key);
		// if (l == null) {
		// l = new ArrayList<String>();
		// }
		// l.add(edgeInfo);
		// initialResult.remove(key);
		// initialResult.put(key, l);

		if (!map.containsKey(key)) {
			List<String> l = new ArrayList<String>();
			l.add(edgeInfo);
			map.put(key, l);
		} else {
			map.get(key).add(edgeInfo);
		}

	}

	@Override
	public void initBeforeSuperStep(SuperStepContextInterface context) {
		map.clear();
		ifile = 1;
		this.superStepCount = context.getCurrentSuperStepCounter();
		// this.jobconf = context.getJobConf();
		this.vertex_num = Integer.valueOf(context.getJobConf().get(
				SubGraphBSP.VERTEX_NUM));
		this.min_support = Integer.valueOf(context.getJobConf().get(
				SubGraphBSP.MIN_SUPPORT));
		if ((superStepCount != 0) && (superStepCount % 2 == 0)) {
			SMFirstAggregateValue subGraph = (SMFirstAggregateValue) context
					.getAggregateValue(SubGraphBSP.SUBGRAPH_INFO);
			LOG.info("superStepCount:" + superStepCount
					+ "[KMeansBSP]******* flag = ********" + flag);
			if(subGraph==null)
				LOG.info("[sjz test] subGraph is null!");
			flag = subGraph.flag;
			LOG.info("changed flag = ********" + flag);
		}

	}

	// private void addToMap(String key, String edgeInfo) {
	// // List<String> l = map.get(key);
	// if (!map.containsKey(key)) {
	// List<String> l = new ArrayList<String>();
	// l.add(edgeInfo);
	// map.put(key, l);
	// }
	// else{
	// map.get(key).add(edgeInfo);
	// }
	//		
	// }
}