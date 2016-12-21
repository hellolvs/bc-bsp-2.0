package com.chinamobile.bcbsp.fault.storage;

import java.util.*;

import javax.mail.*;
import javax.mail.internet.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;


public class EmailSender {
	 private static final Log LOG = LogFactory.getLog(EmailSender.class);
	  /**Get BSP configuration.*/
	  private BSPConfiguration conf = new BSPConfiguration();
	  /**default threshold.*/
	  private String defaultEmailAddress = "zkptestemail@126.com";
	  private String defaultEmailPassword = "zzz000";
	  private String defaultUserAddress = "fengshanbo@foxmail.com";
	 public EmailSender(){
		 
	 }
	 public void sendEmail()
	 {
		  String from = conf.get(
			      Constants.DEFAULT_BC_BSP_JOB_EMAILSENDER_ADDRESS, defaultEmailAddress);
	    //String from="bcbspadministrator@126.com";
	    String password=conf.get(Constants.DEFAULT_BC_BSP_JOB_EMAIL_PASSWORD, defaultEmailPassword);
	    //String to="15140054529@139.com";
	    String to =conf.get(Constants.DEFAULT_BC_BSP_JOB_USER_EMAIL_ADDRESS, defaultUserAddress);
	    LOG.info(from +password +to);
	    String smtpServer="smtp.126.com";
	    String subject="Exception Alters.";
	    String content ="Exception hadppened in your BSP job," +
	    		"please check exception logs to see more details.";
	    Properties props = System.getProperties();
	    props.put("mail.smtp.host", smtpServer);
	    props.put("mail.smtp.auth","true");
	    try{
	    MailAuthenticator autherticator = new MailAuthenticator(from,password);
	    Session session = Session.getDefaultInstance(props,autherticator);	  
	    MimeMessage msg = new MimeMessage(session);	    
	       msg.setFrom(new InternetAddress(from));
	       msg.setRecipient(MimeMessage.RecipientType.TO, new InternetAddress(to));
	       msg.setSubject(subject);
	       msg.setSentDate(new Date());
	       msg.setText(content);
	       Transport.send(msg);
	       LOG.info("Send Email sucessfully!");
	    }catch(Exception e){
	        LOG.error("Exception hadppened during send email!", e);
	    }
	}
}
class MailAuthenticator extends javax.mail.Authenticator{
    
    private String username = null;
    private String userpasswd = null;

    public MailAuthenticator(){}
    public MailAuthenticator(String username,String userpasswd){
        this.username = username;
        this.userpasswd = userpasswd;
    }
    
    public void setUserName(String username){
        this.username = username;
    }

    public void setPassword(String password){
        this.userpasswd = password;
    }

    public PasswordAuthentication getPasswordAuthentication(){
        return new PasswordAuthentication(username,userpasswd);
    }
} 

