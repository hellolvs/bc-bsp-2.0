                          <%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.BSPController"%>
<%@ page import="com.chinamobile.bcbsp.workermanager.WorkerManagerStatus"%>
<%@ page import="com.chinamobile.bcbsp.util.StaffStatus"%>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<%@ page import="java.io.File" %>
<html>
  <head>  
    <title>WorkerManagerDetail.jsp</title>
	<% 
		BSPController bspController =(BSPController)application.getAttribute("bcbspController");
		Collection<WorkerManagerStatus> workerManagersStatus = bspController.workerServerStatusKeySet();
		String workerName= request.getParameter("workerManagerName");
		WorkerManagerStatus thisWorkerStatus = new WorkerManagerStatus();
		WorkerManagerStatus tempWorkerStatus = new WorkerManagerStatus();
		for(Iterator<WorkerManagerStatus> iter = workerManagersStatus.iterator();iter.hasNext();){
			tempWorkerStatus = iter.next();
			if(tempWorkerStatus.getWorkerManagerName().equals(workerName)){
				thisWorkerStatus = tempWorkerStatus;
			}
		}
		session.setAttribute("StaffsStatus",thisWorkerStatus.getStaffReports());
	%>
  </head>  
  <body>
    <div>
		<jsp:include page="bsptop.jsp"/>                            
	</div>
    <div id="main">
        <h4>&nbsp;</h4>
		<h1>WorkerStation:<%=thisWorkerStatus.getWorkerManagerName() %> tasks detail</h1>
		<table border="1" width="60%" style="border-collapse: collapse">
			<tr>
				<td>maxStaffCount</td>
				<td>runningStaffCount</td>
				<td>finishStaffCount</td>
				<td>failStaffCount</td>
			</tr>
			<tr>
				<td><%=thisWorkerStatus.getMaxStaffsCount() %></td>
				<td><%=thisWorkerStatus.getRunningStaffsCount() %></td>
				<td><%=thisWorkerStatus.getFinishedStaffsCount() %></td>
				<td><%=thisWorkerStatus.getFailedStaffsCount() %></td>
			</tr>
			</table><br><hr>
			<h3>All Tasks</h3>
			<table border="1" width="60%" style="border-collapse: collapse">
			<tr>
				<td>StaffID</td>
				<td>JobID</td>
				<td>State</td>
				<td>Process</td>
				<td>MaxSuperStepCount</td>
				<td>TaskLog</td>
			</tr>
		<%
			int i=0;
			BSPConfiguration bspconfiguration=new BSPConfiguration();
			String usrlogsUrl=bspconfiguration.get("bcbsp.userlogs.dir");             
		   
			for(Iterator<StaffStatus> iter = ((List<StaffStatus>)session.getAttribute("StaffsStatus")).iterator();iter.hasNext();){
				StaffStatus st = (StaffStatus)iter.next();
				//change string attempt to staff
				String staffID = st.getStaffId().toString();
				String temp[] = staffID.split("pt"); 
				staffID="staff"+temp[1]; 
				out.print("<tr><td><a href=\"StaffDetail.jsp?taskID="+st.getStaffId()+"\">"+staffID+"</a></td>");
				out.print("<td>"+st.getJobId()+"</td>");
				out.print("<td>"+st.getRunState()+"</td>");
				out.print("<td>"+st.getProgress()+"</td>");
				out.print("<td>"+st.getSuperstepCount()+"</td>"); 

			usrlogsUrl=usrlogsUrl+"/"+st.getStaffId();
			File file = new File(usrlogsUrl);
				out.print("<td><a target='_blank' href='http://"+thisWorkerStatus.getLocalIp()+":40027/readUserLog.jsp?"+file.getAbsoluteFile()+"'>"+"log"+"</a></td></tr>"); 
				i++;
			usrlogsUrl=bspconfiguration.get("bcbsp.userlogs.dir");
			}
			%>
		</table><br><hr>
		<h3>Local logs</h3>
		<a href="<%="http://"+thisWorkerStatus.getLocalIp()+":40027/logUrl.jsp"%>">Log</a> directory
		<a href="<%="http://"+thisWorkerStatus.getLocalIp()+":40027/userLogUrl.jsp"%>">UserLog</a> directory
		<br><hr>
		BC-BSP,2013
	</div>
	<meta http-equiv="refresh" content="5" />
  </body>
</html>

