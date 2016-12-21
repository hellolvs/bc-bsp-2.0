
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
		<title>Subgen Lable Graph Generator</title>
	</head>
	<body>
		<div>
		<jsp:include page="bsptop.jsp"/>                            
		</div><br/>
		<div id="main">
			<h1>
				Subgen Lable Graph Generator 
			</h1>
			<h2>
				Quick Start
			</h2>
			<p>Subgen Lable Graph Generator is the artificial graph generator of <a href="https://ailab.wsu.edu/subdue/">SUBDUE</a>. <br/>
                           The subgen program creates two files. They are 'sample.insts' and 'sample.graph'.
The '.insts' file contains the instances imbedded in the graph along with the deviations (if any) of each instance from the original substructure.
The '.graph' file contains the complete graph in SUBDUE format along with some approximate graph statistics in comments.  
			</p>
                        <p>Before using this graph generator,user should confirm the vertex number, edge number and the seed. The seed should be an integer which will be used by subgen to initialize the random number generator.
           </p>
		   <form name="form" action="subgenGraphGenerator.jsp" method="post"/>  
		   <table>
		   <tr>
		   <td style="color:blue;width:145px;text-align:right">Vertex Number:</td><td><input type=text name="MyInput1" /></td> 
		   </tr>
		   <tr>
		   <td style="color:blue;width:145px;text-align:right">Edge Number:</td><td><input type=text name="MyInput2" /></td>
		   </tr>   
		   <tr> 
		   <td style="color:blue;width:145px;text-align:right">Seed:</td><td><input type=text name="MyInput3" /> </td>
		   </tr>
		   <tr> 
		   <td style="color:blue;width:145px;text-align:right">Output Path:</td><td> <input type=text name="MyInput4" /></td>
		   </tr> 
		   <tr><td></td><td style="color:blue;width:145px;text-align:right"><button type="submit"style="width:85px;color:blue" >RUN</button>  </td></tr>
		   </table>
		<% 
			
		String input1=request.getParameter("MyInput1");
		String input2=request.getParameter("MyInput2");
		String input3=request.getParameter("MyInput3");
		String input4=request.getParameter("MyInput4");
			BSPConfiguration bspconfiguration=new BSPConfiguration();
			String graphdata=bspconfiguration.get("bcbsp.graphdata.dir"); 

			if(input1!=null&&!input1.equals("")){
			try {  

			 Process process = Runtime.getRuntime().exec("bash "+graphdata+"/subgen.sh"+" "+input1+" "+input2+" "+input3+" "+input4);
		 
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

