/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.weibo.search.flume.source.TCP;

import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TcpUtils {
 
//  private Mode m = Mode.START;

  private ByteArrayOutputStream baos;
  private static final Logger logger = LoggerFactory
      .getLogger(TcpUtils.class);
  
  public static final Integer MIN_SIZE = 10;
  public static final Integer DEFAULT_SIZE = 2500;
  
  private final boolean isUdp;
  private boolean isBadEvent;
  private boolean isIncompleteEvent;
  private Integer maxSize;
//  private Set<String> keepFields;

 
  private String msgBody = null;
  private Mode m = Mode.START;
  private StringBuilder header = new StringBuilder();


//  public static final String KEEP_FIELDS_ALL = "--all--";
//
//  public static boolean keepAllFields(Set<String> keepFields) {
//    if (keepFields == null) {
//      return false;
//    }
//    return keepFields.contains(KEEP_FIELDS_ALL);
//  }

//  public static Set<String> chooseFieldsToKeep(String keepFields) {
//    if (keepFields == null) {
//      return null;
//    }
//
//    keepFields = keepFields.trim().toLowerCase(Locale.ENGLISH);
//
//    if (keepFields.equals("false") || keepFields.equals("none")) {
//      return null;
//    }
//
//    if (keepFields.equals("true") || keepFields.equals("all")) {
//      Set<String> fieldsToKeep = new HashSet<String>(1);
//      fieldsToKeep.add(KEEP_FIELDS_ALL);
//      return fieldsToKeep;
//    }
//
////    Set<String> fieldsToKeep = new HashSet<String>(DEFAULT_FIELDS_TO_KEEP.length);
////
////    for (String field : DEFAULT_FIELDS_TO_KEEP) {
////      if (keepFields.indexOf(field) != -1) {
////        fieldsToKeep.add(field);
////      }
////    }
//
////   return fieldsToKeep;
//  }

//  public static String addFieldsToBody(Set<String> keepFields,
//                                       String body,
//                                       String priority,
//                                       String version,
//                                       String timestamp,
//                                       String hostname) {
//    // Prepend fields to be kept in message body.
//    if (keepFields != null) {
//      if (keepFields.contains(SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_HOSTNAME)) {
//        body = hostname + " " + body;
//      }
//      if (keepFields.contains(SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_TIMESTAMP)) {
//        body = timestamp + " " + body;
//      }
//      if (keepFields.contains(SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_VERSION)) {
//        if (version != null && !version.isEmpty()) {
//          body = version + " " + body;
//        }
//      }
//      if (keepFields.contains(SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_PRIORITY)) {
//        body = "<" + priority + ">" + body;
//      }
//    }
//
//    return body;
//  }

  public TcpUtils() {
    this(false);
  }

  public TcpUtils(boolean isUdp) {
	  this(DEFAULT_SIZE, isUdp);
//    this(DEFAULT_SIZE,
//        new HashSet<String>(Arrays.asList(SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS)),
//        isUdp);
  }

  public TcpUtils(Integer eventSize, boolean isUdp) {
    this.isUdp = isUdp;
    isBadEvent = false;
    isIncompleteEvent = false;
    maxSize = (eventSize < MIN_SIZE) ? MIN_SIZE : eventSize;
    baos = new ByteArrayOutputStream(eventSize);
 //   this.keepFields = keepFields;
//    initHeaderFormats();
  }

//  // extend the default header formatter
//  public void addFormats(Map<String, String> formatProp) {
//    if (formatProp.isEmpty() || !formatProp.containsKey(
//        SyslogSourceConfigurationConstants.CONFIG_REGEX)) {
//      return;
//    }
//  
//  }

 

 enum Mode {
	 START, HEADER, DATA
 };

  public enum SyslogStatus {
    OTHER("Unknown"),
    INVALID("Invalid"),
    INCOMPLETE("Incomplete");

    private final String syslogStatus;

    private SyslogStatus(String status) {
      syslogStatus = status;
    }

    public String getSyslogStatus() {
      return this.syslogStatus;
    }
  }

  // create the event from syslog data
  Event buildEvent() {
	  
  //  try {
      byte[] body;
      
     String headerVal = header.toString();
     Map<String, String> headers = new HashMap<String, String>();
     //logger.info("header {}" + headerVal);
     
     if(( headerVal != null) && (headerVal.length() > 0)) {
    	 headers.put("log_type", headerVal);
     }

   
    if ((msgBody != null) && (msgBody.length() > 0)) {
      body = msgBody.getBytes();
    } else {
      // Parse failed.
      body = baos.toByteArray();
    }
      
      // format the message
      // debug
//      logger.info("header {}", header.toString());
//      logger.info("body {}", body.toString());
      reset();
      return EventBuilder.withBody(body, headers);
      
//    } finally {
//      reset();
//    }
    
  }

  
  private void reset() {
    baos.reset();
    m = Mode.START;
    header.delete(0, header.length());
    isBadEvent = false;
    isIncompleteEvent = false;
    msgBody = null;
  }

  // extract relevant log data needed for building Flume event
  public Event extractEvent(ChannelBuffer in) {

    /* for protocol debugging
    ByteBuffer bb = in.toByteBuffer();
    int remaining = bb.remaining();
    byte[] buf = new byte[remaining];
    bb.get(buf);
    HexDump.dump(buf, 0, System.out, 0);
    */
    
    byte b = 0;
    Event e = null;
    boolean doneReading = false;

    try {
      while (!doneReading && in.readable()) {
        b = in.readByte();
        switch(m) {
        	case START:
        		if( b == '<' ) {
//        			baos.write(b);
        			m = Mode.HEADER;
        		} else if ( b == '\n'){
        			logger.debug("Delimiter found while in START mode, ingnoring ....");
        		} else {
//        			// bad event£¬just dump it
//        			baos.write(b);
//        			m = Mode.DATA;
        			in.resetReaderIndex();
        			return null;
        		}
        		break;
        	case HEADER:
//        		baos.write(b);
        		if ( b == '>' ) {
        			if ( header.length() == 0) {
        				// bad event
        				in.resetReaderIndex();
            			return null;
        			}
        			m = Mode.DATA;
        		} else {
        			char ch = (char)  b;
        			header.append(ch);
        			// b should not be ' '
        		}
        		break;
        	case DATA:  	
        		// seperated by '\n'
        		if ( b == '\n' ){
        			e = buildEvent();
        			doneReading = true;
        		} else {
        			baos.write(b);
        		}
        		if (baos.size() == this.maxSize && !doneReading) {
//        			isIncompleteEvent = true;
//        			e = buildEvent();
//        			doneReading = true;    
        			in.resetReaderIndex();
        			return null;
        		}
        		break;
        	}
      }

      // UDP doesn't send a newline, so just use what we received
      if (e == null && isUdp) {
        doneReading = true;
        e = buildEvent();
      }
    } finally {
      // no-op
    }

    return e;
  }

  public Integer getEventSize() {
    return maxSize;
  }

  public void setEventSize(Integer eventSize) {
    this.maxSize = eventSize;
  }

//  public void setKeepFields(Set<String> keepFields) {
//    this.keepFields = keepFields;
//  }
}


