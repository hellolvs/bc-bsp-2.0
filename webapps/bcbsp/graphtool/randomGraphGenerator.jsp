<%@ page contentType="text/html;charset=gb2312" language="java"
	import="javax.script.*,java.sql.*,java.util.List,java.util.ArrayList,java.io.InputStreamReader,java.io.BufferedReader" errorPage=""%>
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
		<title>Random Graph Generator</title>
	</head>
	<body>
		<div>
		<jsp:include page="bsptop.jsp"/>                            
		</div><br/>
		<div id="main">
				<h1>Random Graph Generator   
				</h1>
                         <h2>
				Introduce
			</h2>
			<p>Random Graph Generator is a local C++ executable program of <a href="http://www.cse.psu.edu/~madduri/software/GTgraph/">GTgraph</a>. It takes as input the number of vertices (n) and edges
	(m), and adds m edges randomly choosing a pair of vertices each time. There is a
	possibility of adding multiple edges between a pair of vertices in this case.
	Self loops are not allowed by default, but this can be changed by modifying the appropriate <br></p><p>
	    <br>Before using this graph generator,user should confirm the vertex number and edge number for G(n,m). <br>  
	    </p>
				<h2>
					Quick Start
				</h2>
			<form name="form" action="randomGraphGenerator.jsp" method="post"/>  
		   <table>
		   <tr>
		   <td style="color:blue;width:145px;text-align:right">Vertex Number:</td><td><input type=text name="MyInput1" /></td> 
		   </tr>
		   <tr>
		   <td style="color:blue;width:145px;text-align:right">Edge Number:</td><td><input type=text name="MyInput2" /></td>
		   </tr>   
		   <tr> 
		   <td style="color:blue;width:145px;text-align:right">Output Path:</td><td><input type=text name="MyInput3" /> </td>
		   </tr>
		   <tr><td></td><td style="color:blue;width:145px;text-align:right"><button type="submit"style="width:85px;color:blue" >RUN</button>  </td></tr>
		   </table>
                   <a href="graphGenerator.jsp">Back To Chose Graph Generator.</a>
			  <!-- <form name="form" action="randomGraphGenerator.jsp" method="post" >
			   <tr>
			   <td>Vertex Number&nbsp; :</td>
			   <input type=text name="MyInput1" />   
			   </tr>
			   <tr>
			   <td>Edge Number&nbsp; :</td>
			   <input type=text name="MyInput2" /> 
			   </tr>   
			   <tr> 
			   <td>Output Path&nbsp; :</td>
			   <input type=text name="MyInput3" /> 
			   </tr>
		  
			   <button type="submit">submit</button>  
			</form>

			-->
			<% 
			String input1=request.getParameter("MyInput1");
			String input2=request.getParameter("MyInput2");
			String input3=request.getParameter("MyInput3");
				BSPConfiguration bspconfiguration=new BSPConfiguration();
       	String graphdata=bspconfiguration.get("bcbsp.graphdata.dir");   
        if(input1!=null&&!input1.equals("")){
				try {  

				 Process process = Runtime.getRuntime().exec("bash "+graphdata+"/GT-Radom.sh"+" "+input1+" "+input2+" "+input3);
         process.waitFor();
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
