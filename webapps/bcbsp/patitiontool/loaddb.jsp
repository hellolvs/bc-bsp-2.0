<%@ page contentType="text/html; charset=gb2312" language="java" import="java.io.*" errorPage="" %>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=gb2312" />
<title>无标题文档</title>
</head>
<body>
<div>
		<jsp:include page="bsptop.jsp"/>                            
		</div><br/>
<% 
   BSPConfiguration bspconfiguration=new BSPConfiguration();
   String recourcecode =bspconfiguration.get("bcbsp.graphpartool.dir");
   File loadFile=new File(recourcecode+"/realsourcetest.rar");
   
   response.setHeader("Content-disposition","attachment;filename="+"realsourcetest.rar");
  
   response.setContentType("application/msword");
   
   long fileLength=loadFile.length();
   
   String length=String.valueOf(fileLength);
   response.setHeader("content_Length",length);
    
   FileInputStream clientFile=new FileInputStream(loadFile);
  
   OutputStream serverFile=response.getOutputStream();

   
   int n=0;
   byte b[]=new byte[1000000];
   while((n=clientFile.read(b))!=-1)
   {
       serverFile.write(b,0,n);
   }
   serverFile.close();
   clientFile.close();
%>
</body>
</html>

