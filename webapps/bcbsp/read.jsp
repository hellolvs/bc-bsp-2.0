<%@ page language="java" import="java.util.*" pageEncoding="utf-8"%>
<%@ page import="java.io.BufferedReader" %>
<%@ page import="java.io.InputStreamReader" %>
<%@ page import="java.io.FileInputStream" %>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <head>
    <base href="<%=basePath%>">  
    <title>readlogs</title>  
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
    <%
  		StringBuffer sb = new StringBuffer("");
  		String urlString = request.getRequestURL()+ "?" + request.getQueryString();//获取当前页面的url
  		String url = urlString.substring(urlString.indexOf("?")+1);//截取文件读取路径
  		String urlText = url.substring(url.indexOf(".")+1);//获取文件类型
        InputStreamReader read = new InputStreamReader (new FileInputStream(url),"utf-8");
        BufferedReader br = new BufferedReader(read);
        String str = null;
        while((str = br.readLine())!=null){
             sb.append(str+"<br/>");
           } 
        out.println(sb);//打印文件内容
  		%>
     </div>
  </body>
</html>
