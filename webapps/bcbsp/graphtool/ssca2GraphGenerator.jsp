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
		<title>SSCA #2 Graph Generator</title>
	</head>
	<body>
		<div>
		<jsp:include page="bsptop.jsp"/>                            
		</div><br/>
		<div id="main">
				<h1>SSCA #2 Graph Generator   
				</h1>
                        <h2>
				Introduce
			</h2>
			<p>SSCA#2 Graph Generator is a local C++ executable program of <a href="http://www.cse.psu.edu/~madduri/software/GTgraph/">GTgraph</a>.  This generator produces graphs used in the DARPA HPCS SSCA#2 benchmark. TheSSCA#2 graph is directed with integer edge weights, and made up of random-sized cliques,
	with a hierarchical inter-clique distribution of edges based on a distance metric. An SSCA#2
	clique is defined as a maximal set of vertices, where each pair of vertices is connected by
	directed edges in one or both directions.
	 <br></p><p> 
	    <br>The following parameters can be specified by the user. The default values of each parameter are also given below.<br>  
	    <br>&bull; SCALE: An integer value used to express the rest of the graph parameters. The problem size can be increased by increasing the value of SCALE.&nbsp;</p><p>
			<br>&bull; TotVertices: The number of vertices (2^SCALE ) <br></p><p>&bull; MaxCliqueSize: The maximum number of vertices in a clique. Clique sizes are distributed uniformly on [1, MaxCliqueSize] ([2^(SCALE/3.0)])</p><p>&bull; ProbUnidirectional: Probability that the connections between two vertices will be unidirectional as opposed to bidirectional (0.2)</p><p>&bull; MaxParallelEdges: The maximum number of parallel edges from one vertex to another. The actual number is distributed uniformly on [1, MaxParallelEdges] (3)</p><p>&bull; ProbIntercliqueEdges: Initial probability of an interclique edges (0.5)<br>&bull; MaxIntWeight: Maximum value of integer edge weight. The weights are distributed uniformly on [1, MaxIntWeight] (2^SCALE )</p><p>Before using this graph generator,user should confirm the parameter SCALE value.<br></p>
				<h2>
					Quick Start
				</h2>
				
			<form name="form" action="ssca2GraphGenerator.jsp" method="post"/>  
		   <table>
		   <tr>
		   <td style="color:blue;width:145px;text-align:right">SCALE value:</td><td><input type=text name="MyInput1" /></td> 
		   </tr>
		   <tr>
		   <td style="color:blue;width:145px;text-align:right">Output Path:</td><td><input type=text name="MyInput2" /></td>
		   </tr>   
		   <tr><td></td><td style="color:blue;width:145px;text-align:right"><button type="submit"style="width:85px;color:blue" >RUN</button>  </td></tr>
		   </table>
                <a href="graphGenerator.jsp">Back To Chose Graph Generator.</a>
			   <!--<form name="form" action="ssca2GraphGenerator.jsp" method="post"/>  
			   <tr>
			   <td>SCALE value&nbsp; :</td>
			   <input type=text name="MyInput1" />   
			   </tr>
			   
			   <tr> 
			   <td>Output Path&nbsp; :</td>
			   <input type=text name="MyInput2" /> 
			   </tr>
		  
			   <button type="submit">submit</button>  
			   -->
			
			<% 
			String input1=request.getParameter("MyInput1");
			String input2=request.getParameter("MyInput2");
				BSPConfiguration bspconfiguration=new BSPConfiguration();
				String graphdata=bspconfiguration.get("bcbsp.graphdata.dir"); 
        if(input1!=null&&!input1.equals("")){  
				try {  

				 Process process = Runtime.getRuntime().exec("bash "+graphdata+"/GT-SSCA2.sh"+" "+input1+" "+input2);
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
