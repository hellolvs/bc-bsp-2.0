<%@ page language="java" import="java.util.*" import="neu.bsp.test.*" import="neu.bsp.status.*" pageEncoding="UTF-8"%>
<%@ page import="com.chinamobile.bcbsp.util.StaffStatus"%>
<html>
  <head> 
    <title>JobStaffDetail.jsp</title>   
	<%
		StaffStatus staffs = (StaffStatus)session.getAttribute("StaffsStatus");
		String staffID = staffs.getStaffId().toString();
		String temp[] = staffID.split("pt"); 
		staffID="Staff"+temp[1]; 
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
			   <td><%=staffs.getJobId()%></td>
		   </tr>
		   <tr>
			   <td>State:</td>
			   <td><%=staffs.getRunState() %></td>
		   </tr>
		   <tr>
			   <td>Process:</td>
			   <td><%=staffs.getProgress() %></td>
		   </tr>
		   <tr>
			   <td>MaxSuperStepCount:</td>
			   <td><%=staffs.getSuperstepCount() %></td>
		   </tr>
		   <tr>
			   <td>StartTime:</td>
			   <td><%=new Date(staffs.getStartTime()).toString()%></td>
		   </tr>
		   <tr>
			   <td>FinishTime:</td>
			   <td><%=new Date(staffs.getFinishTime()).toString()%></td>
		   </tr>
	   </table>
	   <hr>
	   BC-BSP,2013
	</div>
	<meta http-equiv="refresh" content="5" />
  </body>
</html>
