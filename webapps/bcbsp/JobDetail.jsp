<%@ page language="java" import="java.util.*" import="neu.bsp.test.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.BSPController"%>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<%@ page import="com.chinamobile.bcbsp.util.JobStatus"%>
<%@ page import="com.chinamobile.bcbsp.util.BSPJobID"%>
<%@ page import="com.chinamobile.bcbsp.util.StaffAttemptID"%>
<%@ page import="com.chinamobile.bcbsp.util.StaffStatus"%>
<%@ page import="java.util.ArrayList"%>
<%@page import="org.apache.hadoop.io.Writable"%>
<%@page import="org.apache.hadoop.io.LongWritable"%>
<%@page import="org.apache.hadoop.io.Text"%>
<%@page import="java.util.HashMap"%>
<%@ page import="java.io.File" %>
<html>
  <head>   
  	<style type="text/css">
		.Tab2 td{ border:solid 1px #3CBFFF}
 	</style>
    <title>JobDetail</title>   
	<%
		BSPController bspController =(BSPController)application.getAttribute("bcbspController");
		//get job status
		JobStatus[] jobs=bspController.getAllJobs();
		String jobID = request.getParameter("jobID");
		BSPJobID bspJobID = new BSPJobID().forName(jobID);
		JobStatus jobStatus = bspController.getJobStatus(bspJobID);	
		//get staffstatus of job
		HashMap<StaffAttemptID, StaffStatus> mapstaffs=bspController.getStaffs();
 		Iterator<Writable>iteratorStaffs=jobStatus.getMapStaffStatus().values().iterator();
		ArrayList<StaffStatus> staffs=new ArrayList<StaffStatus>();
		while(iteratorStaffs.hasNext()){
		     StaffStatus ss = (StaffStatus)iteratorStaffs.next();
		     staffs.add(ss);
		}
	%>
  </head>  
  <body>
    <div>
		<jsp:include page="bsptop.jsp"/>                            
	</div>
	<div id="main" style="overflow:auto;">
	   <h4>&nbsp; <br></h4>
	   <h1>Job Details</h1><hr>
	   <table>
		   <tr>
			   <td><b>JobID:</b></td>
			   <td><%=jobStatus.getJobID() %></td>
		   </tr>
		   <tr>
			   <td><b>State:</b></td>
			   <td><%=jobStatus.getState() %></td>
		   </tr>
		   <tr>
			   <td><b>User:</b></td>
			   <td><%=jobStatus.getUsername() %></td>
		   </tr>
		   <tr>
			   <td><b>Progress:</b></td>
			   <td><%=jobStatus.progress() %></td>
		   </tr>
		   <tr>
			   <td><b>MaxSuperStepCount:</b></td>
			   <td><%=jobStatus.getSuperstepCount() %></td>
		   </tr>
		   <%
	        if(jobStatus.getMapcounter().size()==5){
	        	Iterator<Writable>iterator=jobStatus.getMapcounter().values().iterator();
				ArrayList<Long> counters=new ArrayList<Long>();
				while(iterator.hasNext()){
		      		LongWritable counlong = (LongWritable)iterator.next();
		       		counters.add(counlong.get());
				}
				out.print("<tr><td><b>TIME_IN_SYNC_MS:</b></td><td>"+counters.get(1)+"</td></tr>");
				out.print("<tr><td><b>MESSAGE_BYTES_SENT:</b></td><td>"+counters.get(0)+"</td></tr>");
				out.print("<tr><td><b>MESSAGES_NUM_RECEIVED:</b></td><td>"+counters.get(2)+"</td></tr>");
				out.print("<tr><td><b>MESSAGES_NUM_SENT:</b></td><td>"+counters.get(3)+"</td></tr>");
				out.print("<tr><td><b>MESSAGE_BYTES_RECEIVED:</b></td><td>"+counters.get(4)+"</td></tr>");
	        }
			%>
 			<tr>
			   <td><b>StartTime:</b></td>
			   <td><%=new Date(jobStatus.getStartTime()).toString() %></td>
		   </tr>
		   <tr>
			   <td><b>FinishTime:</b></td>
			   <td><%=new Date(jobStatus.getFinishTime()).toString() %></td>
		   </tr>
	   </table>
	   <hr><br/>
	   <h4>&nbsp; <br></h4>
	   <h1>Staff List</h1>
	   <h4>&nbsp; <br></h4>
	   <table  border="1" width="98%" style="border-collapse: collapse" class="Tab2">
	   		<tr>
	   		<td style='width:220px;height:32px;text-align:center;'>StaffID</td>
	   		<td style='width:80px;height:32px;text-align:center;'>WorkerManger</td>
	   		<td style='width:120px;height:32px;text-align:center;'>RunState</td>
	   		<td style='width:80px;height:32px;text-align:center;'>SuperStepNum</td>
	   		</tr>
		 
		   	<%
	   		   
				for(int i=0;i<jobStatus.getMapStaffStatus().size();i++){
				out.print("<tr>");
				StaffAttemptID attemptid=staffs.get(i).getStaffId();
				StaffStatus jobToStaff=mapstaffs.get(staffs.get(i).getStaffId());
				session.setAttribute("StaffsStatus",jobToStaff);
				//add for change attempt to staff
				String staffID =jobToStaff.getStaffId().toString();
				String temp[] = staffID.split("pt"); 
				staffID="Staff"+temp[1]; 
				//output stafflist
				out.print("<td style='width:220px;height:32px;text-align:center;'><a href=\"JobStaffDetail.jsp?taskID="+staffs.get(i).getStaffId()+"\">"+staffID+"</a></td>");
				//out.print("<td style='width:220px;height:32px;text-align:center;'>"+staffs.get(i).getStaffId()+"</td>");
				out.print("<td style='width:80px;height:32px;text-align:center;'>"+jobToStaff.getWorkerManager()+"</td>");
				out.print("<td style='width:120px;height:32px;text-align:center;'>"+jobToStaff.getRunState()+"</td>");
				out.print("<td style='width:80px;height:32px;text-align:center;'>"+jobToStaff.getProgress()+"</td>");		
				out.print("</tr>");
				}
			%>
		 
	   		
	   </table>
	   <h4>&nbsp; <br></h4>
	   BC-BSP,2013
	 </div> 
	 <meta http-equiv="refresh" content="5" />
</body>
</html>
