package com.chinamobile.bcbsp.examples.copra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class C_Aggregator {
	public static final Log LOG = LogFactory.getLog(C_Aggregator.class);

	public static String aggregate(ArrayList<String> aggValues) {
		
		String aggV ;
		Map<String, Long> community_VertexNum = new HashMap<String, Long>();
		
		Iterator<String> it = aggValues.iterator();
		while (it.hasNext()) {
			combine(it.next(), community_VertexNum);
		}
		
		aggV=getStringValue(community_VertexNum);
		
		return aggV;
	}
	public static void combine(String aggV, Map<String, Long> map) {
		
		String value = aggV;
	//	LOG.info("wyj:"+value+":");
		String[] community_VertexNum = value.split("\\|");
		String[] cvs;
		for(int i = 0; i != community_VertexNum.length; ++ i) {
			cvs = community_VertexNum[i].split("-");
			if(cvs.length==2){
			if(map.containsKey(cvs[0])) {
				map.put(cvs[0], map.get(cvs[0])+ Long.parseLong(cvs[1]));
			} else {
				map.put(cvs[0], Long.parseLong(cvs[1]));
			}
			}else{
				LOG.info("wyj:"+community_VertexNum[i]);
			}
		}
	}
	
	public static String getStringValue(Map<String, Long> map) {
		Iterator<String> it = map.keySet().iterator();
		StringBuffer sb = new StringBuffer();
		String label;
		
		while(it.hasNext()) {
			label = it.next();
			sb.append(label + "-" + map.get(label) + "|");
		}
		
		return sb.toString();
	}
}