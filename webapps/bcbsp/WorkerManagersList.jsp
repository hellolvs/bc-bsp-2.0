<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.BSPController"%>
<%@ page import="com.chinamobile.bcbsp.workermanager.WorkerManagerStatus"%>
<html>
  <head> 
    <title>NodesList</title>  
        <style>
	    table tr td{ text-align:center; }
	</style>
	<% 
		BSPController bspController =(BSPController)application.getAttribute("bcbspController");
		Collection<WorkerManagerStatus> workerManagersStatus = bspController.workerServerStatusKeySet();
	%>
  </head> 
  <body>
    <div>
		<jsp:include page="bsptop.jsp"/>                            
	</div>
    <div id="main" style="overflow-y:auto;">
    <h4>&nbsp;</h4>
    <h1>BS-BSP Machine List</h1>
    <h4>&nbsp;</h4>
    <h3>Worker Managers</h3>
    <table border="1" width="60%" style="border-collapse: collapse">
	    <tr>
		    <td>WorkerManagerName</td>
		    <td>maxStaffCount</td>
		    <td>runningStaffCount</td>
		    <td>finishStaffCount</td>
		    <td>failStaffCount</td>
	    </tr>
	    <%
	    	WorkerManagerStatus status;
	    	for(Iterator<WorkerManagerStatus> iter=workerManagersStatus.iterator();iter.hasNext();){
	    	 	status=iter.next();
	    		out.print("<tr>");
	    		out.print("<td><a href=\"WorkerManagerDetail.jsp?workerManagerName="+status.getWorkerManagerName()+"\">"+status.getWorkerManagerName()+"</a></td>");
	    		out.print("<td>"+status.getMaxStaffsCount()+"</td>");
	    		out.print("<td>"+status.getRunningStaffsCount()+"</td>");
	    		out.print("<td>"+status.getFinishedStaffsCount()+"</td>");
	    		out.print("<td>"+status.getFailedStaffsCount()+"</td>");
	    		out.print("</tr>");
	    	} 	
	    %>
    </table>
    <br><hr>
    BC-BSP,2013
	</div>
	<meta http-equiv="refresh" content="5" />
  </body>
</html>
