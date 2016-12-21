<%@ page contentType="text/html;charset=gb2312" language="java"
	import="java.sql.*,java.util.List,java.util.ArrayList,java.io.InputStreamReader,java.io.BufferedReader" errorPage=""%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<jsp:directive.page import="java.io.FileReader"/>
<jsp:directive.page import="javax.swing.text.Document"/>
<jsp:directive.page import="java.text.SimpleDateFormat"/>
<jsp:directive.page import="java.util.Calendar"/>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
</head>
<body>
	<div>
	<jsp:include page="bsptop.jsp"/>                            
	</div><br/>
	<div id="main">
			<h1>User Operation Center</h1>
	 	 	<div>
			<form name="form" action="UserCenter.jsp" method="post"/>  
				<label>UserProgram:</label> <input type="text" name="MyBsp"/>
				<label>SuperStep:</label><input type="text" name="SuperStep" />
				<label>User-defined1:</label><input type="text"style="width:120px" />
				<label>InputData:</label><input type="text" name="InputData"/>
				<label>FragementSize:</label><input type="text" name="FragementSize"/>
				<label>User-defined2:</label><input type="text" style="width:120px"/>
				<label>OutputPath:</label><input type="text" name="OutputPath"/>
				<label>StaffNumber:</label><input type="text" name="StaffNumber"/>
				<label>User-defined2:</label><input type="text" style="width:120px"/>
				<button type="submit">RUN</button> </td>
					<% 
			
					String bspProgram=request.getParameter("MyBsp");
					String superstep=request.getParameter("SuperStep");
					String inputdata=request.getParameter("InputData");
					String outputpath=request.getParameter("OutputPath");
                    String fragementSize=request.getParameter("FragementSize");
					String staffNumber=request.getParameter("StaffNumber");

					BSPConfiguration bspconfiguration=new BSPConfiguration();
					String graphdata=bspconfiguration.get("bcbsp.graphdata.dir"); 

					if(bspProgram!=null&&!bspProgram.equals("")){
					try {  
					 out.print("123");
					 out.print("<script>alert('Finish!');</script>");
					 Process process = Runtime.getRuntime().exec("bash "+graphdata+"/bspprograme.sh"+""+bspProgram+" "+superstep+" "+inputdata+" "+outputpath+" "+fragementSize+" "+staffNumber);
				 
					 while(true)
					{
					if(process.waitFor()==0)
						{
					 out.print("<script>alert('Succeed!');</script>");
					 break;
					}     
					}  
					} catch (Exception e) {  
						e.printStackTrace();  
					}  
			}  
		

		%>

				</tr>


	 	</div>
		<div style="height:50px">
		<span style="height:50px">
         <textarea rows="23px" cols="143px" style="background-color:LightGrey"></textarea>
		</span>	
	 	</div>
</div>
</body>
</html>