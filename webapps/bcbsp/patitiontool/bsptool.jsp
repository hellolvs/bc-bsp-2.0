<%@ page contentType="text/html; charset=gb2312" language="java" import="java.io.*" errorPage="" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<meta http-equiv="Content-Type"
			content="text/html; charset=gb2312">
		<title>BSP tool Instruction</title>
	</head>
	<body>
		<div>
		<jsp:include page="bsptop.jsp"/>                            
		</div><br/>
		<div id="main">
			<h1>Ranger Numbering 
		</h1>
			<h2>
				Introduce
			</h2>
			<p>可以直接作为划分工具使用，给不同的分区数据进行连续的顶点编号,也可以作为其他划分工具的后续编号处理。  
	    </p>
	    <h2>
				DP(Distributed Partition)：
			</h2>
			<p>分布式的划分工具，根据启发式的划分算法进行划分，具有一定程度的拓扑密集型。  
	    </p>
	    <h2>
			DBP(Distributed with Bucket Partition)：
			</h2>
			<p>分布式的划分工具，根据启发式划分算法进行划分，并进行虚拟桶重组，具有一定的拓扑密集型。  
	    </p>
			<br>
			<a href="loadrange.jsp">StartTo get range numbering code.</a>
			<br>
			<a href="loaddb.jsp">StartTo get db&&dbp  code.</a>
		<br><br></body>
	  </div>
</html>
