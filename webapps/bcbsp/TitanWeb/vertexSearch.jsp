<%@ page contentType="text/html;charset=gb2312" language="java"
	import="java.sql.*,java.util.List,java.util.ArrayList,java.io.InputStreamReader,java.io.BufferedReader,java.util.StringTokenizer,java.lang.StringBuffer" errorPage=""%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<%@ page import="com.tinkerpop.rexster.client.RexProException"%>
<%@ page import="com.tinkerpop.rexster.client.RexsterClient"%>
<%@ page import="com.tinkerpop.rexster.client.RexsterClientFactory"%>
<%@ page import="java.io.IOException"%>
<%@ page import="java.util.List"%>
<%@ page import="java.util.Map"%>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=gb2312" />	
	<meta http-equiv="Pragma" content="no-cache"/>
	<link rel="Stylesheet" href="CSS/Container3.css" />
	<script type="text/javascript" src="JS/wbbm1.js"></script>
	<script type="text/javascript" src="JS/Container3.js"></script>
		<title>GraphSerach</title>
	</head>
	<body>
	<div>
		<jsp:include page="bsptop.jsp"/>                            
	</div>
	<div id="main">
		   <%
		  		String graphNames = (String)session.getAttribute("graphNames");	
		  		
		    %>
		   <h6>&nbsp;</h6><br/>
	    	<h2 style="color:#3CBFFF">Vertex Search:<%=graphNames%></h2>
	    	<h5>&nbsp;</h5>
	    	<h2 style="color:#3CBFFF">Please input the vertex number here</h2>
	    	<h6>&nbsp;</h6><br/>
		<form name="form" action="vertexSearch.jsp" method="post"> 
		<table>
			<tr>
			<td style="width:20px;text-align:right;font-size:12pt;color:#2D9EDB">From:</td>
			<td><input type="text" id="vertexid1" name="vertexid1" style="border:1px solid #3CBFFF;height:30px" /></td>
			<td style="width:20px;text-align:right;font-size:12pt;color:#2D9EDB">To:</td>
			<td><input type="text" id="vertexid2" name="vertexid2" style="border:1px solid #3CBFFF;height:30px" /></td>
			<td><button type="submit" value="VertexID" style='background:url(Images/Draw_Button_8.png);padding:0px 0 0px 0;border:0;width:150px;height:36px;' value='Button'>search</button></td>			
			</tr>
			<tr><td colspan="1"></td></tr>
		</table>
		</form>
		<h5>&nbsp;</h5>
			<h2 style="color:#3CBFFF">Operation Result</h2>
		<div style="border:2px solid #2D9EDB;width:1022px;height:520px">
		   <%
		   if(graphNames!=null&&graphNames!=" "){		
		// out.print(str);
		try{
			String vertexID1=request.getParameter("vertexid1");
			String vertexID2=request.getParameter("vertexid2");
			BSPConfiguration bspconfiguration=new BSPConfiguration();
			String bspmasterIp=bspconfiguration.get("bsp.http.bspmaster.ip"); 
			RexsterClient client = RexsterClientFactory.open(bspmasterIp,graphNames);
			List<Map<String, Object>> result;

			// out.print("<tr><td><a href=\"vertexSearch.jsp?graphName="+vertexID+"\"></td>");   
		   	if(vertexID1!=null&&vertexID2!=null){ 
		   	long vertexNum1 = Integer.parseInt(vertexID1);
		   	long vertexNum2 = Integer.parseInt(vertexID2);
		   	for( long i=vertexNum1;i<=vertexNum2;i++){
		   	 result = client.execute("g.V('vertexID','" + i + "').map");
		    String searchResult = result.toString();
		   	// out.print("<td>"+str +"</td>");
		   	 out.print(searchResult);}
		   	 }
		   	 client.close();
		   	 } catch (Exception e) {
			e.printStackTrace();
		} 
		}else{
		String graphNames1 = (String)session.getAttribute("graphNames");
		
		try{
			out.print(graphNames1);
			String vertexID1=request.getParameter("vertexid1");
			String vertexID2=request.getParameter("vertexid2");
			BSPConfiguration bspconfiguration=new BSPConfiguration();
			String bspmasterIp=bspconfiguration.get("bsp.http.bspmaster.ip"); 
			RexsterClient client = RexsterClientFactory.open(bspmasterIp,graphNames1);
			List<Map<String, Object>> result;

			// out.print("<tr><td><a href=\"vertexSearch.jsp?graphName="+vertexID+"\"></td>");   
		   	if(vertexID1!=null&&vertexID2!=null){ 
		   	long vertexNum1 = Integer.parseInt(vertexID1);
		   	long vertexNum2 = Integer.parseInt(vertexID2);
		   	for( long i=vertexNum1;i<=vertexNum2;i++){
		   	 result = client.execute("g.V('vertexID','" + i + "').map");
		    String searchResult = result.toString();
		   	// out.print("<td>"+str +"</td>");
		   	 out.print(searchResult);}
		   	 }
		   	 client.close();
		   	 } catch (Exception e) {
			e.printStackTrace();
		} 
		}
		%>
		 </div>
		</div>   
	</body>

</html>
