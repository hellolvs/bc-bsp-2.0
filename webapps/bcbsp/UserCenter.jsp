<%@ page contentType="text/html;charset=gb2312" language="java"
	import="java.sql.*,java.util.List,java.util.ArrayList,java.io.InputStreamReader,java.io.BufferedReader,java.util.StringTokenizer,java.lang.StringBuffer" errorPage=""%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<jsp:directive.page import="java.io.FileReader"/>
<jsp:directive.page import="javax.swing.text.Document"/>
<jsp:directive.page import="java.text.SimpleDateFormat"/>
<jsp:directive.page import="java.util.Calendar"/>
<%@ page import="com.chinamobile.bcbsp.BSPConfiguration"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.BSPController"%>
<%@ page import="com.chinamobile.bcbsp.bspcontroller.ClusterStatus"%>
<%@ page import="com.chinamobile.bcbsp.util.JobStatus"%>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>

	<meta http-equiv="Pragma" content="no-cache"/>
	<link rel="Stylesheet" href="CSS/Container3.css" />
	<script type="text/javascript" src="JS/wbbm1.js"></script>
	<script type="text/javascript" src="JS/Container3.js"></script>
	<style type="text/css">
		body{font:"ËÎÌו";font-size:16px;}
		a:link,a:visited{font-size:16px;color:#666;text-decoration:none;}
		a:hover{color:#ff0000;text-decoration:underline;}
		#Tab{margin:0 auto;width:1024px;border:1px solid #BCE2F3;}
		.Menubox{height:38px;border-bottom:1px solid #64B8E4;background:#E4F2FB;}
		.Menubox ul{list-style:none;margin:7px 40px;padding:0;position:absolute;}
		.Menubox ul li{float:left;background:#64B8E4;line-height:30px;display:block;cursor:pointer;width:165px;text-align:center;color:#fff;font-weight:bold;border-top:1px solid #64B8E4;border-left:1px solid #64B8E4;border-right:1px solid #64B8E4;}
		.Menubox ul li.hover{background:#fff;border-bottom:1px solid #fff;color:#147AB8;}
		.Contentbox{clear:both;margin-top:0px;border-top:none;height:181px;padding-top:8px;height:100%;}
		.Contentbox ul{list-style:none;margin:7px;padding:0;}
		.Contentbox ul li{line-height:24px;border-bottom:1px dotted #ccc;}
		.Tab2 td{ border:solid 1px #3CBFFF}
 	</style>
 		<%	
			BSPController bspController =(BSPController)application.getAttribute("bcbspController");
			ClusterStatus clusterStatus=bspController.getClusterStatus(false);
			JobStatus[] jobs=bspController.getAllJobs();
		%>
 	<script>
		function setTab(name,cursel,n){
			for(i=1;i<=n;i++){
			var menu=document.getElementById(name+i);
			var con=document.getElementById("con_"+name+"_"+i);
			menu.className=i==cursel?"hover":"";
			con.style.display=i==cursel?"block":"none";
			}
		}
		function myrefresh() 
		{
			timeHandler=setTimeout("myrefresh()",10000);
		} 
		function stopfresh() 
		{
		if(timeHandler!=undefined)
			{
			clearTimeout(timeHandler);
			timeHandler=undefined;
			}
		} 
 	</script>
</head>
<body>
	<div>
		<jsp:include page="bsptop.jsp"/>                            
	</div>
	<div >
		<form name="form" action="UserCenter.jsp" method="post"/> 
		<div id="Tab">
			<div class="Menubox">
			<ul>
				<li id="menu1" onmouseover="setTab('menu',1,2)" class="hover">simpleInput</li>
				<li id="menu2" onmouseover="setTab('menu',2,2)" >user-defined</li>
			</ul>
			</div>
		    <div class="Contentbox"> 
			    <div id="con_menu_1" class="hover">
			      <ul>
			        <li>
				        <div style="width:1023px;height:570px;"> 
				         	<div>   
								<table>
									<tr><td style="width:110px;text-align:right;font-size:12pt;color:#2D9EDB">UserProgram:</td><td><input type="file" name="MyBsp" style="border:1px solid #3CBFFF;height:30px" /></td></tr>
									<tr><td style="width:110px;text-align:right;font-size:12pt;color:#2D9EDB">SuperStepNum:</td><td><input type="text" name="SuperStep" style="border:1px solid #3CBFFF;height:20px" /></td></tr>
									<tr><td style="width:110px;text-align:right;font-size:12pt;color:#2D9EDB">InputDataPath:</td><td><input type="text" name="InputData" style="border:1px solid #3CBFFF;height:20px" /></td></tr>
									<tr><td style="width:110px;text-align:right;font-size:12pt;color:#2D9EDB">OutputPath:</td><td><input type="text" name="OutputPath" style="border:1px solid #3CBFFF;height:20px" /></td></tr>
									<tr><td style="width:110px;text-align:right;font-size:12pt;color:#2D9EDB">FragementSize:</td><td><input type="text" name="FragementSize" style="border:1px solid #3CBFFF;height:20px" /></td></tr>
									<tr><td style="width:110px;text-align:right;font-size:12pt;color:#2D9EDB">StaffNumber:</td><td><input type="text" name="StaffNumber" style="border:1px solid #3CBFFF;height:20px" /></td></tr>
									<tr><td style="width:110px;text-align:right;font-size:12pt;color:#2D9EDB">UserDefined1:</td><td><input type="text" name="UserDefined1" style="border:1px solid #3CBFFF;height:20px" /></td></tr>
									<tr><td style="width:110px;text-align:right;font-size:12pt;color:#2D9EDB">UserDefined2:</td><td><input type="text" name="UserDefined2" style="border:1px solid #3CBFFF;height:20px" /></td></tr>
									<tr><td style="width:110px;text-align:right;font-size:12pt;color:#2D9EDB">UserDefined3:</td><td><input type="text" name="UserDefined3" style="border:1px solid #3CBFFF;height:20px" /></td></tr>
									<tr></tr><tr></tr><tr></tr>
									<tr>
									<td style="width:110px;text-align:center">
									<button type="submit" style='background:url(Images/Draw_Button_9.png);padding:0px 0 0px 0;border:0;width:85px;height:28px;' value='Button'>SUBMIT</button>
									</td>
									</tr>
							   </table>
						   </div>
						   <div>
						   			<h5>&nbsp;</h5><br /><h5>&nbsp;</h5>
								    <h2>Jobs Summary</h2>
								    <h6>&nbsp;</h6>
								    <!--  <table>
								    	<tr>
								    		<td><button onclick="myrefresh()" style="background:url(Images/Draw_Button_9.png);padding:0px 0 0px 0;border:0;width:85px;height:28px;">autoFresh</button></td>
								    		<td colspan="1"></td>
								    		<td><button onclick="stopfresh()" style="background:url(Images/Draw_Button_9.png);padding:0px 0 0px 0;border:0;width:85px;height:28px;">stopFresh</button></td>
										</tr>
									</table>
									-->
								    <table border="1" width="60%" style="border-collapse: collapse" class="Tab2">
									    <tr>
										    <td>JobID</td>
										    <td>State</td>
										    <td>User</td>
										    <td>Progress</td>
										    <td>MaxSuperStepCount</td>
									    </tr>
										    <%for(int i=0;i<jobs.length;i++){ 
													out.print("<tr><td><a href=\"JobDetail.jsp?jobID="+jobs[i].getJobID()+"\">"+jobs[i].getJobID()+"</a></td>");
													out.print("<td>"+jobs[i].getState()+"</td>");;
													out.print("<td>"+jobs[i].getUsername()+"</td>");
													out.print("<td>"+(jobs[i].progress())+"</td>");
													out.print("<td>"+(jobs[i].getSuperstepCount())+"</td>");
													
											}%>
								    </table>
						   </div>
				        </div>
				        
			        </li>
			      </ul>
			    </div>	
		    	<div id="con_menu_2" style="display:none">
		      		<ul>
				        <li>
					        <div style="width:1023px;height:570px;">
					        		<table>
					        			<tr><td style="text-align:left;font-size:12pt;color:#2D9EDB">UserProgram:<input type="file" name="MyBsp2" style="border:1px solid #3CBFFF;height:30px" /></td></tr>
										<tr><td><textarea cols="40"rows="8"style="OVERFLOW:hidden;border:1px solid #3CBFFF" id="UserDefined" name="UserDefined"></textarea></td></tr>
										<tr></tr><tr></tr><tr></tr>
										<tr>
										<td style="width:110px;text-align:center">
										<button type="submit" style='background:url(Images/Draw_Button_8.png);padding:0px 0 0px 0;border:0;width:150px;height:36px;' value='Button'>SUBMIT</button>
										</td>
										</tr>										
								   </table>
					        </div>
				        </li>
		      		</ul>
		    	</div>
  			</div>
		</div>
	</div>
<% 
					//pagerank example
					String bspProgram=request.getParameter("MyBsp");
					String superstep=request.getParameter("SuperStep");
					String inputdata=request.getParameter("InputData");
					String outputpath=request.getParameter("OutputPath");
                    String fragementSize=request.getParameter("FragementSize");
					String staffNumber=request.getParameter("StaffNumber");
					String userdefined1=request.getParameter("UserDefined1");
					String userdefined2=request.getParameter("UserDefined2");
					String userdefined3=request.getParameter("UserDefined3");
					BSPConfiguration bspconfiguration=new BSPConfiguration();
					String graphdata=bspconfiguration.get("bcbsp.graphdata.dir"); 
					if(bspProgram!=null&&!bspProgram.equals("")){
						Process process = Runtime.getRuntime().exec("bash "+graphdata+"/bspUserDefinedNew.sh"+" "+bspProgram+" "+superstep+" "+inputdata+" "+outputpath+" "+fragementSize+" "+staffNumber+" "+userdefined1+" "+userdefined2+" "+userdefined3);
						//Process process = Runtime.getRuntime().exec("bash /usr/bc-bsp-0.1/graphdata/bspprograme.sh bcbsp-examples.jar 5 inputbsp output 64 4"); 
                   }  
					//user-defined 	
					String bspProgramBrowse=request.getParameter("MyBsp2");
					String userDefined=request.getParameter("UserDefined");			
					if(userDefined!=null&&!userDefined.equals("")){
						Process process = Runtime.getRuntime().exec("bash "+graphdata+"/bspUserDefinedNew.sh "+bspProgramBrowse+" "+userDefined); 
                    }  

		%>
	<div>
	</div>
</body>
</html>