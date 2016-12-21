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
		<title>GraphSerach</title>
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
	    	<h2 style="color:#3CBFFF">Complicated Search:<%=graphNames%></h2>
	    	<h2 style="color:#3CBFFF">Please input the search comand here</h2>
			<form name="form" action="complicatedSearch.jsp" method="post"/> 
			<table width="974" height="143">
			<tr><td><textarea rows="4" cols="60" id="vertexid1" name="vertexid1" style="OVERFLOW:hidden;border:2px solid #3CBFFF"></textarea></td><td><span style="color: rgb(51, 51, 51); font-family: Helvetica,arial,freesans,clean,sans-serif; font-size: 15px; font-style: normal; font-variant: normal; font-weight: normal; letter-spacing: normal; line-height: 25.5px; text-align: start; text-indent: 0px; text-transform: none; white-space: normal; word-spacing: 0px; background-color: rgb(255, 255, 255); display: inline ! important; float: none;">Gremlin is a graph traversal language.<br />More details please see:https://github.com/tinkerpop/gremlin/wiki<br /></span> </td></tr>
			<tr><td><button type="submit" value="complicatedSearch" style='background:url(Images/Draw_Button_8.png);padding:0px 0 0px 0;border:0;width:150px;height:36px;'>commit</button></td></tr>
			<tr><td><h5>&nbsp;</h5></td></tr>
			</table>
			<h5>&nbsp;</h5>
			<h2 style="color:#3CBFFF">Operation Result</h2>
			<div style="border:2px solid #2D9EDB;width:1022px;height:420px">
			   <%
			  
			   	 
			    if(graphNames!=null&&graphNames!=" "){		
			   try{
				String searchCommand=request.getParameter("vertexid1");
				BSPConfiguration bspconfiguration=new BSPConfiguration();
	            String bspmasterIp=bspconfiguration.get("bsp.http.bspmaster.ip"); 
			    RexsterClient client = RexsterClientFactory.open(bspmasterIp,graphNames);
				List<Map<String, Object>> result;			
			   	if(searchCommand!=null){ 
			   	result = client.execute(searchCommand);
			    String searchResult = result.toString();
			   	 out.print(searchResult);
			   	}
			   	 client.close();
			   	 } catch (Exception e) {
				e.printStackTrace();
			} 
			}else{
			String graphNames1 = (String)session.getAttribute("graphNames");
			
					
			   try{				
				String searchCommand=request.getParameter("vertexid1");
				BSPConfiguration bspconfiguration=new BSPConfiguration();
			String bspmasterIp=bspconfiguration.get("bsp.http.bspmaster.ip"); 
			RexsterClient client = RexsterClientFactory.open(bspmasterIp,graphNames);
				List<Map<String, Object>> result;				
			   	if(searchCommand!=null){ 			   	
			   out.print(graphNames);
			   	result = client.execute(searchCommand);
			    String searchResult = result.toString();
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
