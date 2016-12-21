<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.BSPController"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.ClusterStatus"%>
<%@ page import="com.chinamobile.bcbsp.util.JobStatus"%>
<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<title>Monitor</title>
		<%	
			BSPController bspController =(BSPController)application.getAttribute("bcbspController");
			ClusterStatus clusterStatus=bspController.getClusterStatus(false);
			JobStatus[] jobs=bspController.getAllJobs();
		%>
	</head>
	<body>
		<div>
		<jsp:include page="bsptop.jsp"/>                            
		</div>
		<div id="main">
			<h4>&nbsp;</h4>
		 	<h1>BSPController</h1>
		 	<h6>&nbsp;</h6>
		    <b>State:</b><%=clusterStatus.getBSPControllerState() %>
		    <h6>&nbsp;</h6>
		    <h3>CLuster Summary</h3>
		    <h6>&nbsp;</h6>
		    <table border="1" width="60%" style="border-collapse: collapse">
			    <tr>
				    <td>Active WorkerManager</td>
				    <td>currentRunningStaff</td>
				    <td>Staff Capacity</td>
			    </tr>
			    <tr>
				    <td><a href="WorkerManagersList.jsp"><%=clusterStatus.getActiveWorkerManagersCount()%></a></td>
				    <td><%=clusterStatus.getRunningClusterStaffs()%></td>
				    <td><%=clusterStatus.getMaxClusterStaffs()%></td>
			    </tr>
		    </table><br/><hr/>
		    <h5>&nbsp;</h5>
		    <h3>Jobs Summary</h3>
		    <h6>&nbsp;</h6>
		    <table border="1" width="60%" style="border-collapse: collapse">
			    <tr>
				    <td>JobID</td>
				    <td>State</td>
				    <td>User</td>
				    <td>Progress</td>
				    <td>MaxSuperStepCount</td>
			    </tr>
				    <%for(int i=0;i<jobs.length;i++){ 
							out.print("<tr><td><a href=\"JobDetail.jsp?jobID="+jobs[i].getJobID()+"\">"+jobs[i].getJobID()+"</a></td>");
							out.print("<td>"+jobs[i].getState()+"</td>");
							out.print("<td>"+jobs[i].getUsername()+"</td>");
							out.print("<td>"+(jobs[i].progress())+"</td>");
							out.print("<td>"+(jobs[i].getSuperstepCount())+"</td>");
							
					}%>
		    </table><br/><hr/>
		    <h5>&nbsp;</h5> 
		    <h3>Local logs</h3>
		    <h6>&nbsp;</h6>
		    <a href="logUrl.jsp">Log</a> directory<hr/>
		    BC-BSP,2013	
		</div>
		<meta http-equiv="refresh" content="5" />
	</body>
</html>
