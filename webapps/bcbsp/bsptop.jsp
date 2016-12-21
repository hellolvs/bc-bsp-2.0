<%@ page contentType="text/html; charset=GBK" %>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<link rel="Stylesheet" href="CSS/Menu2.css" />
<script type="text/javascript" src="JS/wbbm.js"></script>
<script type="text/javascript" src="JS/Menu2.js"></script>
<style type="text/css">
</style>
<% 
	BSPConfiguration bspconfiguration=new BSPConfiguration();
	//configure gangliaJSP
	String gangliaIp=bspconfiguration.get("bsp.http.ganglia.ip"); 
	String gangliaServer="http://"+gangliaIp+"/ganglia/";
	//configure graphgeneratorJSP£¬partitionToolJSP
	String bspmasterIp=bspconfiguration.get("bsp.http.bspmaster.ip"); 
	String httpPort=bspconfiguration.get("bsp.http.infoserver.port");
	String garphGenerator="http://"+bspmasterIp+":"+httpPort+"/graphtool/graphGenerator.jsp";
	String bspPartitionTool="http://"+bspmasterIp+":"+httpPort+"/patitiontool/bsptool.jsp";
	//configure hdfs
	String hdfsUrl="http://"+bspmasterIp+":50070"+"/dfshealth.jsp";
	//configure bspController,browse,logUrl,home,usercenter,Titan
	String bspHome="http://"+bspmasterIp+":"+httpPort+"/home.jsp";
	String bspMaster="http://"+bspmasterIp+":"+httpPort+"/BspController.jsp";
	String bspBrowse="http://"+bspmasterIp+":"+httpPort+"/browse.jsp";
	String bspLogUrl="http://"+bspmasterIp+":"+httpPort+"/logUrl.jsp";
	String bspUserCenter="http://"+bspmasterIp+":"+httpPort+"/UserCenter.jsp";
	String bspTitan="http://"+bspmasterIp+":"+httpPort+"/TitanWeb/Titan.jsp";
%>
<div id="top"><div id="txt">BC-BSP Management Platform</div></div>
<div id=Menu2>
	<div class=m_l_2>
		<ul>
			<li>
				<a></a>
			</li>
			<li class>
				<a href=<%=bspHome%>>Home</a>
			</li>
			<li>
				<a >Monitor</a>
				<ul>
					<li >
						<a href=<%=bspMaster%> >BspController</a>
					</li>
					<li>
						<a href=<%=bspBrowse%>>FaultBrowse</a>
					</li>
					<li>
						<a href=<%=bspLogUrl%>>LogManagement</a>
					</li>
					<li>
						<a href=<%=gangliaServer%>>ClusterMonitor</a>
					</li>
				</ul>
			</li>

			<li>
				<a href=<%=bspUserCenter%>>Usercenter</a>
			</li>
			<li>
				<a >Dataview</a>
				<ul>
					<li>
						<a href=<%=hdfsUrl%>>FileSystem</a>
					</li>
					<li>
						<a href=<%=bspTitan%>>Titan</a>
					</li>
				</ul>
			</li>
			<li>
				<a >Tool</a>
				<ul>
					<li>
						<a href=<%=garphGenerator%>>GraphGenerator</a>
					</li>
					<li>
						<a href=<%=bspPartitionTool%>>PatitionTool</a>
					</li>
				</ul>
			</li>		
		</ul>
	</div>
</div>
