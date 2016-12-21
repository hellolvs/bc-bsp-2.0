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
		<link rel="Stylesheet" href="CSS/Container3.css" />
		<script type="text/javascript" src="JS/wbbm1.js"></script>
		<script type="text/javascript" src="JS/Container3.js"></script>
		<title>OutedgeSerach</title>
	</head>
	<body>
		   <%
		  		String graphNames = (String)session.getAttribute("graphNames");	
		    %>
		 <div>
			<jsp:include page="bsptop.jsp"/>                            
	    </div>
	    <div id="main">
	    	<h6>&nbsp;</h6><br/>
	    	<h2 style="color:#3CBFFF">InNeighbor Search:<%=graphNames%></h2>
	    	<h5>&nbsp;</h5>
	    	<h2 style="color:#3CBFFF">Please input the vertex number here</h2>
	    	<h6>&nbsp;</h6><br/>
			<form name="form" action="inEdgeSearch.jsp" method="post"> 
			<table>
				<tr>
				<td><input type="text" id="vertexid1" name="vertexid1" style="border:1px solid #3CBFFF;height:30px" /></td>		
				<td><button type="submit" value="inEdgeSearch" style='background:url(Images/Draw_Button_8.png);padding:0px 0 0px 0;border:0;width:150px;height:36px;'>search</button></td>				
				</tr>
			</table>
			</form>
			<h5>&nbsp;</h5>
			<h2 style="color:#3CBFFF">Operation Result</h2>
			<div style="border:2px solid #2D9EDB;width:1022px;height:490px">
		   <%
		  
		   	 
		    if(graphNames!=null&&graphNames!=" "){		
		   try{
			String vertexID1=request.getParameter("vertexid1");
			BSPConfiguration bspconfiguration=new BSPConfiguration();
	            String bspmasterIp=bspconfiguration.get("bsp.http.bspmaster.ip"); 
			    RexsterClient client = RexsterClientFactory.open(bspmasterIp,graphNames);
			List<Map<String, Object>> result;

		   	if(vertexID1!=null){ 
		   	long vertexNum1 = Integer.parseInt(vertexID1);
		   	//long vertexNum2 = Integer.parseInt(vertexID2);
		   out.print(graphNames);
		   	 result = client.execute("g.V('vertexID','" + vertexNum1 + "').in.map");
		    String searchResult = result.toString();
		   	// out.print("<td>"+str +"</td>");
		   	 out.print(searchResult);
		   	}
		   	 client.close();
		   	 } catch (Exception e) {
			e.printStackTrace();
		} 
		}else{
		String graphNames1 = (String)session.getAttribute("graphNames");
		
		try{
			out.print(graphNames);
			String vertexID1=request.getParameter("vertexid1");
			BSPConfiguration bspconfiguration=new BSPConfiguration();
	            String bspmasterIp=bspconfiguration.get("bsp.http.bspmaster.ip"); 
			    RexsterClient client = RexsterClientFactory.open(bspmasterIp,graphNames);
			List<Map<String, Object>> result;
		   	if(vertexID1!=null){ 
		   	long vertexNum1 = Integer.parseInt(vertexID1);
		   	//long vertexNum2 = Integer.parseInt(vertexID2);
		   
		   	 result = client.execute("g.V('vertexID','" + vertexNum1 + "').in.map");
		    String searchResult = result.toString();
		   	// out.print("<td>"+str +"</td>");
		   	 out.print(searchResult);
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