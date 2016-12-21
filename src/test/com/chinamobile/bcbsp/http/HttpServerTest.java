
package com.chinamobile.bcbsp.http;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.webapp.WebAppContext;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.util.BSPJob;

public class HttpServerTest {
  private String name;
  private String bindAddress;
  private int port;
  private Configuration conf;
  private HttpServer httpServer;
  
  @Before
  public void setUp() throws Exception {
    name = "bcbsp";
    bindAddress = "Master.Hadoop";
    port = 40026;
    conf = new Configuration();
    conf.set("bsp.http.infoserver.webapps", "/usr/bc-bsp-0.1/webapps");
  }
  
  @Test
  public void testHttpServerStringStringIntBooleanConfiguration()
      throws IOException {
    httpServer = new HttpServer(name, bindAddress, port, true, conf);
    assertEquals(
        "Use name, bindAddress, port, findPortFlag, conf to comstruct the HttpServer.",
        true, httpServer.isFindPort());
  }
  
  @Test
  public void testCreateBaseListener() throws IOException {
    httpServer = new HttpServer(name, bindAddress, port, true, conf);
    SelectChannelConnector ret = (SelectChannelConnector) httpServer
        .createBaseListener(conf);
    assertEquals("Test ceateBaseListener method in HttpServer. ", 128,
        ret.getAcceptQueueSize());
    assertEquals("Test ceateBaseListener method in HttpServer. ", 10000,
        ret.getLowResourceMaxIdleTime());
    assertEquals("Test ceateBaseListener method in HttpServer. ", false,
        ret.getUseDirectBuffers());
  }
  
  @Test
  public void testAddDefaultApps() throws IOException {
    ContextHandlerCollection contexts = new ContextHandlerCollection();
    String appDir = conf.get("bsp.http.infoserver.webapps",
        "/usr/bc-bsp-0.1/webapps");
    httpServer = new HttpServer(name, bindAddress, port, true, conf);
    httpServer.addDefaultApps(contexts, appDir);
    assertEquals("Test addDefaultApps method in HttpServer. ", 2, httpServer
        .getDefaultContexts().size());
  }
  
  @Test
  public void testAddContextContextBoolean() throws IOException {
    WebAppContext webAppCtx = new WebAppContext();
    httpServer = new HttpServer(name, bindAddress, port, true, conf);
    httpServer.addContext(webAppCtx, true);
    assertEquals("Test addContext method in HttpServer. ", 2, httpServer
        .getDefaultContexts().size());
  }
  
  @Test
  @Ignore(value = "the size of filterNames is always 0.")
  public void testAddFilterPathMapping() throws IOException {
    String pathSpec = "/stacks";
    httpServer = new HttpServer(name, bindAddress, port, true, conf);
    WebAppContext webAppCtx = new WebAppContext();
    httpServer.addFilterPathMapping(pathSpec, webAppCtx);
  }
  
  @Test
  public void testStartStop() throws Exception {
    BSPController controller = mock(BSPController.class);
    httpServer = new HttpServer(name, bindAddress, port, true, conf);
    httpServer.setAttribute("bspcontroller", controller);
    httpServer.start();
    assertEquals("Test Start method in HttpServer. ", true, httpServer
        .getWebServer().isRunning());
    httpServer.stop();
    assertEquals("Test stop method in HttpServer. ", false, httpServer
        .getWebServer().isRunning());
  }
    
}
