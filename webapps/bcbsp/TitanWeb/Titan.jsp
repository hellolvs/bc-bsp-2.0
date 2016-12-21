<%@ page contentType="text/html;charset=gb2312" language="java"
	import="java.sql.*,java.util.List,java.util.ArrayList,java.io.InputStreamReader,java.io.BufferedReader,java.util.StringTokenizer,java.lang.StringBuffer" errorPage=""%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<jsp:directive.page import="java.io.FileReader"/>
<jsp:directive.page import="javax.swing.text.Document"/>
<jsp:directive.page import="java.text.SimpleDateFormat"/>
<jsp:directive.page import="java.util.Calendar"/>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.BSPController"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.ClusterStatus"%>
<%@ page import="com.chinamobile.bcbsp.util.JobStatus"%>
<%@ page import="com.chinamobile.bcbsp.io.titan.*"%>
<%@ page import="java.io.IOException"%>
<%@ page import="java.util.List"%>
<%@ page import="java.util.Map"%>

<%@ page import="org.apache.commons.logging.Log"%>
<%@ page import="org.apache.commons.logging.LogFactory"%>
<%@ page import="org.apache.hadoop.io.Text"%>

<%@ page import="com.chinamobile.bcbsp.io.RecordWriter"%>
<%@ page import="com.chinamobile.bcbsp.util.BSPJob"%>
<%@ page import="com.tinkerpop.rexster.client.RexProException"%>
<%@ page import="com.tinkerpop.rexster.client.RexsterClient"%>
<%@ page import="com.tinkerpop.rexster.client.RexsterClientFactory"%>
<%@ page import="org.apache.hadoop.conf.Configuration"%>
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
		<%  
			String str=request.getParameter("sb"); 
		%>
		<div>
			<jsp:include page="bsptop.jsp"/>                            
		</div>
		<div id="main">
		
		<form name="form" action="Titan.jsp" method="post"/> 
		
		<table>
			<tr>
			<h2 style="color:#3CBFFF">Please input graphname here!</h2>
			<h5>&nbsp;</h5>
			<td><input type="text" id="sb" name="sb" style="border:1px solid #3CBFFF;height:30px"/></td>
			<td><button type="submit" value="graphcomfirm" style='background:url(Images/Draw_Button_8.png);padding:0px 0 0px 0;border:0;width:150px;height:36px;'>Confirm Graphname</button></td>	
		    </tr>
		    </table>
		    <table>
		   <%
			
		   //try {
		 
		   if(str!=null){
		   out.println("<tr><td>"+"Graph"+": "+str+" has been comfirmed"+"</td></tr>");
		   out.println("<tr><td colspan='1'style='height:26px'></td></tr>");
		   out.println("<tr><td><a href=\"vertexSearch.jsp?graphName="+str+"\"><h3 style='color:#3CBFFF'>Vertex Information Search</h3></td></tr>");
		   out.println("<tr><td colspan='1'style='height:26px'></td></tr>");
		   out.println("<tr><td><a href=\"edgeSearch.jsp?graphName="+str+"\"><h3 style='color:#3CBFFF'>OutNeighborSearch</h3></td></tr>");
		   out.println("<tr><td colspan='1'style='height:26px'></td></tr>");
		   out.println("<tr><td><a href=\"inEdgeSearch.jsp?graphName="+str+"\"><h3 style='color:#3CBFFF'>InNeighborSearch</h3></td></tr>");
		   out.println("<tr><td colspan='1'style='height:26px'></td></tr>");
		   out.println("<tr><td><a href=\"complicatedSearch.jsp?graphName="+str+"\"><h3 style='color:#3CBFFF'>ComplicatedSearch</h3></td></tr>");
		   }
		   session.setAttribute("graphNames",str);
		    %>
		
		</table>
		</div>
	</body>

</html>
