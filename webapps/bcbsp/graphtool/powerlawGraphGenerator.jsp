
<%@ page contentType="text/html;charset=gb2312" language="java"
	import="java.sql.*,java.util.List,java.util.ArrayList,java.io.InputStreamReader,java.io.BufferedReader" errorPage=""%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<jsp:directive.page import="java.io.FileReader"/>
<jsp:directive.page import="javax.swing.text.Document"/>
<jsp:directive.page import="java.text.SimpleDateFormat"/>
<jsp:directive.page import="java.util.Calendar"/>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<html>
	<head>
		<meta http-equiv="Content-Type"
			content="text/html; charset=ISO-8859-1">
		<title>Power-law Graph Generator</title>
	</head>
	<body>
		<div>
		<jsp:include page="bsptop.jsp"/>                            
		</div><br/>
		<div id="main">
			<h1>Power-law Graph Generator   
			</h1>
                        <h2>
				Introduce
			</h2>
			<p>Power-law Graph Generator is a Map-Reduce job which can generate graph data with power-law. 
			<br>The power-law formula is as follow:
			<br>
			<br>y = c * [x^(-r)]
			<br>
			<br>The parameters of c and r is confirmed by user. 
			<h2>
				Quick Start
			</h2>
			
			<form name="form" action="powerlawGraphGenerator.jsp" method="post"/>  
		   <table>
		   <tr>
		   <td style="color:blue;width:145px;text-align:right">Vertex Number:</td><td><input type=text name="MyInput1" /></td> 
		   </tr>
		   <tr>
		   <td style="color:blue;width:145px;text-align:right">Output Path:</td><td><input type=text name="MyInput2" /></td>
		   </tr>   
		   <tr> 
		   <td style="color:blue;width:145px;text-align:right">Parameter c:</td><td><input type=text name="MyInput3" /> </td>
		   </tr>
		   <tr> 
		   <td style="color:blue;width:145px;text-align:right">Parameter r:</td><td> <input type=text name="MyInput4" /></td>
		   </tr> 
		   <tr><td></td><td style="color:blue;width:145px;text-align:right"><button type="submit"style="width:85px;color:blue" >RUN</button>  </td></tr>
		   </table>
                <a href="graphGenerator.jsp">Back To Chose Graph Generator.</a>
		  <!-- <form name="form" action="powerlawGraphGenerator.jsp" method="post"/> 
		   <table>
		   <tr>
		   <td>Vertex Number:</td>
		   <input type=text name="MyInput1" />   
		   </tr>
		   <tr>
		   <td>Output Path&nbsp; :</td>
		   <input type=text name="MyInput2" /> 
		   </tr>   
		   <tr> 
		   <td>Parameter c&nbsp; :</td>
		   <input type=text name="MyInput3" /> 
		   </tr>
		   <tr> 
		   <td>Parameter r&nbsp; :</td>
		   <input type=text name="MyInput4" /> 
		   </tr>
	  
		   <button type="submit">submit</button> 
		   </table>

		</form>

		-->
		<% 
		String input1=request.getParameter("MyInput1");
		String input2=request.getParameter("MyInput2");
		String input3=request.getParameter("MyInput3");
		String input4=request.getParameter("MyInput4");
			BSPConfiguration bspconfiguration=new BSPConfiguration();
			String graphdata=bspconfiguration.get("bcbsp.graphdata.dir");   

			if(input1!=null&&!input1.equals("")){
			try {  

		 Process process = Runtime.getRuntime().exec("bash "+graphdata+"/powerlawgraph.sh"+" "+input1+" "+input2+" "+input3+" "+input4);     

			 while(true)
			{
			if(process.waitFor()==0)
				{
			out.print("<script>alert('Finish!');</script>");
			 break;
			}     
			}  
			} catch (Exception e) {  
				e.printStackTrace();  
			}  
	  
		
	}
		%>
   </div>
		
 </body>
</html>

