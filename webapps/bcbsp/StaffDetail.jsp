<%@ page language="java" import="java.util.*" import="neu.bsp.test.*" import="neu.bsp.status.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.util.StaffStatus"%>
<html>
  <head> 
    <title>StaffDetail.jsp</title>   
	<%
		List<StaffStatus> staffs = (List<StaffStatus>)session.getAttribute("StaffsStatus");
		String staffName = request.getParameter("taskID");
		StaffStatus thisStaffStatus = new StaffStatus();
		StaffStatus tempStaffStatus = new StaffStatus();
		for(Iterator<StaffStatus> iter=staffs.iterator();iter.hasNext();){
			tempStaffStatus = iter.next();
			if(tempStaffStatus.getStaffId().toString().equals(staffName)){
				thisStaffStatus=tempStaffStatus;
			}
		}
		String staffID = thisStaffStatus.getStaffId().toString();
		String temp[] = staffID.split("pt"); 
		staffID="staff"+temp[1]; 
	%>
  </head>
  
  <body>
	<div>
		<jsp:include page="bsptop.jsp"/>                            
	</div>
	<div id="main">
	   <h4>&nbsp;</h4>
	   <h1>Staff Details</h1><hr>
	   <table>
		   <tr>
			   <td>StaffID:</td>
			   <td><%=staffID%></td>
		   </tr>
		   <tr>
			   <td>JobID:</td>
			   <td><%=thisStaffStatus.getJobId()%></td>
		   </tr>
		   <tr>
			   <td>State:</td>
			   <td><%=thisStaffStatus.getRunState() %></td>
		   </tr>
		   <tr>
			   <td>Process:</td>
			   <td><%=thisStaffStatus.getProgress() %></td>
		   </tr>
		   <tr>
			   <td>MaxSuperStepCount:</td>
			   <td><%=thisStaffStatus.getSuperstepCount() %></td>
		   </tr>
		   <tr>
			   <td>StartTime:</td>
			   <td><%=new Date(thisStaffStatus.getStartTime()).toString()%></td>
		   </tr>
		   <tr>
			   <td>FinishTime:</td>
			   <td><%=new Date(thisStaffStatus.getFinishTime()).toString()%></td>
		   </tr>
	   </table>
	   <hr>
	   BC-BSP,2013
	</div>
	<meta http-equiv="refresh" content="5" />
  </body>
</html>
