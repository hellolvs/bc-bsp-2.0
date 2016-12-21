<%@ page language="java" import="java.util.*" pageEncoding="utf-8"%>
<%@ page import="java.io.FileInputStream" %>
<%@ page import="java.io.BufferedReader" %>
<%@ page import="java.io.InputStreamReader" %>
<%@ page import="java.io.File" %>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<jsp:directive.page import="java.io.FileReader"/>
<jsp:directive.page import="javax.swing.text.Document"/>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path;
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <head>
    <base href="<%=basePath%>"> 
    <title>userLogUrl</title>
	<meta http-equiv="pragma" content="no-cache">
	<meta http-equiv="cache-control" content="no-cache">
	<meta http-equiv="expires" content="0">    
	<meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
	<meta http-equiv="description" content="This is my page">
  </head>
  <body>
    <div>
		<jsp:include page="bsptop.jsp"/>                            
	</div>
	<div id="main">
	  <h4>&nbsp;</h4>
      <h1>Directory:/logs/userlogs</h1>
       <%                   
           BSPConfiguration bspconfiguration=new BSPConfiguration();
       	   String usrlogsUrl=bspconfiguration.get("bcbsp.userlogs.dir");             
           File file = new File(usrlogsUrl);       
           if(file.exists()){        	  
                File[] chiled_file =  file.listFiles();        	  
                for(int i = 0;i<chiled_file.length;i++){        		      		 
                    File f = chiled_file[i];  
                    //新开页面并显示文本内容 
                    out.println("<h4>&nbsp;</h4>") ;
                    out.println("<a target='_blank' href='readUserLog.jsp?"+f.getAbsoluteFile()+"'>"+f.getName()+f.lastModified()+"</a>") ;
                    out.println("<br/>");        		 
                }          
            }    
        %>
	</div>
  </body>
</html>
