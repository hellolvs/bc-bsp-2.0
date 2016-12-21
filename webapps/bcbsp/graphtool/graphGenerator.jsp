<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<meta http-equiv="Content-Type"
			content="text/html; charset=ISO-8859-1">
		<title>graph data dispose</title>
	</head>
	<body>
		<div>
			<jsp:include page="bsptop.jsp"/>                            
		</div><br/>
		<div id="main">
			<h1>
				Graph Generators
			</h1>
			<h2>
				Introduce
			</h2>
			<p>	
		There are
			<B>Six</B> graph generators for BC-BSP Jobs.They can generate five types of directed graphs:power-law graph, normal graph, R-MAT power-law graph, SSCA#2 graph and random graph. You can read the instructions of them and use them in the list.
			</p>Among them, R-MAT power-law graph generator, SSCA#2 graph generator and random graph generator come from <a href="http://www.cse.psu.edu/~madduri/software/GTgraph/">GTgraph</a>. We change the format of the output to adjacency list.
                        <p>Subgen Graph Generator is the artificial graph generator of <a href="https://ailab.wsu.edu/subdue/">SUBDUE</a>. And it can generate the graph with vertex lable and edge lable.
                        <p>		
			<h2>Graph Generators List</h2>
			<table width="729" height="124">
			<tr> <td ><a href="powerlawGraphGenerator.jsp">Power-law Graph Generator</a></td> </tr>
			<tr> <td ><a href="normalGraphGenerator.jsp">Normal Graph Generator</a></td> </tr>		
			<tr> <td ><a href="rmatGraphGenerator.jsp">R-MAT Graph Generator</a></td> </tr>	
			<tr> <td ><a href="ssca2GraphGenerator.jsp">SSCA#2 Graph Generator</a></td> </tr>	
			<tr> <td ><a href="randomGraphGenerator.jsp">Random Graph Generator:</a></td> </tr>
			<tr> <td ><a href="subgenGraphGenerator.jsp">Subgen Graph Generator:</a></td> </tr>		
					</table>
				<br>
	     </div>
	</body>
</html>
