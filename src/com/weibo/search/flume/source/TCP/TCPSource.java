/**
 * 
 */
package com.weibo.search.flume.source.TCP;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Title: TCPSource.java
 * @author ewalker.zj@gmail.com
 * @date 2017Äê3ÔÂ6ÈÕ
 */
public class TCPSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger logger = LoggerFactory.getLogger(TCPSource.class);

	  private int port;
	  private String host = null;
	  private Channel nettyChannel;
	  private Integer eventSize;
	  private SourceCounter sourceCounter;
	  public class tcpHandler extends SimpleChannelHandler {

	    private  final Pattern IGNORANCE_ERROR_MESSAGE = Pattern.compile("^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$", Pattern.CASE_INSENSITIVE);

		/* (non-Javadoc)
		 * @see org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ExceptionEvent)
		 */
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			// TODO Auto-generated method stub
//			logger.error("Caught exception ", e);
//			e.getCause();
			
			Throwable cause = e.getCause();
			if( cause instanceof IOException){
				if(ignoreException(cause)){
					return;
				}
			}
			
			Channel ch = e.getChannel();
			ch.close();
			
			super.exceptionCaught(ctx, e);
		}

		private boolean ignoreException(Throwable t) {
			// TODO Auto-generated method stub
			if( t instanceof IOException){
				String message = String.valueOf(t.getMessage()).toLowerCase();
				if (IGNORANCE_ERROR_MESSAGE.matcher(message).matches()){
					return true;
				}
			}
			return false;
		}

		private TcpUtils tcpUtils = new TcpUtils();

	    public void setEventSize(int eventSize) {
	      tcpUtils.setEventSize(eventSize);
	    }

//	    public void setKeepFields(Set<String> keepFields) {
//	      tcpUtils.setKeepFields(keepFields);
//	    }
//
//	    public void setFormater(Map<String, String> prop) {
//	      tcpUtils.addFormats(prop);
//	    }

	    @Override
	    public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
//	      Channel ch = ctx.getChannel();
//	      if(ch.isOpen()&&ch.isConnected()){
	    	  ChannelBuffer buff = (ChannelBuffer) mEvent.getMessage();
	    	  
	    	  //for debug
	    	  System.out.println("**"+buff.toString(Charset.defaultCharset())+"**");
		      
		      while (buff.readable()) {
		        Event e = tcpUtils.extractEvent(buff);
		        if (e == null) {
		          logger.debug("Parsed partial event, event will be generated when " +
		              "rest of the event is received.");
		          continue;
		        }
		        
		        sourceCounter.incrementEventReceivedCount();
		        //logger.info("read tcp message");
		        try {
		          getChannelProcessor().processEvent(e);
		          sourceCounter.incrementEventAcceptedCount();
		        } catch (ChannelException ex) {
		          logger.error("Error writting to channel, event dropped", ex);
		        } catch (RuntimeException ex) {
		          logger.error("Error parsing event from tcp stream, event dropped", ex);
		          return;
		        }
		      }  
	    	  
//	      }
	      
	      

	    }
	  }

	  @Override
	  public void start() {
	    ChannelFactory factory = new NioServerSocketChannelFactory(
	        Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

	    ServerBootstrap serverBootstrap = new ServerBootstrap(factory);
	 
	    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
	      @Override
	      public ChannelPipeline getPipeline() {
	    	  ChannelPipeline pipeline = Channels.pipeline();
	    	  tcpHandler handler = new tcpHandler();
	    	  handler.setEventSize(eventSize);
//	    	  handler.setFormater(formaterProp);
//	    	  handler.setKeepFields(keepFields);
	    	  
	    	  ChannelBuffer delimiter = ChannelBuffers.wrappedBuffer(new byte[] {'\n'});
			
	    	  pipeline.addLast("frameDecoder", new DelimiterBasedFrameDecoder(1024*10, false, delimiter));
	    	  pipeline.addLast("handler", handler);
	    	  return Channels.pipeline(handler);
	      }
	    });

	    logger.info("TCP Source starting...");

	    if (host == null) {
	      nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
	    } else {
	      nettyChannel = serverBootstrap.bind(new InetSocketAddress(host, port));
	    }

	    sourceCounter.start();
	    super.start();
	  }

	  @Override
	  public void stop() {
	    logger.info("TCP Source stopping...");
	    logger.info("Metrics: {}", sourceCounter);

	    if (nettyChannel != null) {
	      nettyChannel.close();
	      try {
	        nettyChannel.getCloseFuture().await(60, TimeUnit.SECONDS);
	      } catch (InterruptedException e) {
	        logger.warn("netty server stop interrupted", e);
	      } finally {
	        nettyChannel = null;
	      }
	    }

	    sourceCounter.stop();
	    super.stop();
	  }

	  @Override
	  public void configure(Context context) {
	    Configurables.ensureRequiredNonNull(context,
	        TcpSourceConfigurationConstants.CONFIG_PORT);
	    port = context.getInteger(TcpSourceConfigurationConstants.CONFIG_PORT);
	    host = context.getString(TcpSourceConfigurationConstants.CONFIG_HOST);
	    eventSize = context.getInteger("eventSize", TcpUtils.DEFAULT_SIZE);
	    
	    // default: none
//	    keepFields = TcpUtils.chooseFieldsToKeep(
//	        context.getString(
//	            SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS,
//	            SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS));

	    if (sourceCounter == null) {
	      sourceCounter = new SourceCounter(getName());
	    }
	  }

	  @VisibleForTesting
	  InetSocketAddress getBoundAddress() {
	    SocketAddress localAddress = nettyChannel.getLocalAddress();
	    if (!(localAddress instanceof InetSocketAddress)) {
	      throw new IllegalArgumentException("Not bound to an internet address");
	    }
	    return (InetSocketAddress) localAddress;
	  }


	  @VisibleForTesting
	  SourceCounter getSourceCounter() {
	    return sourceCounter;
	  }
}
