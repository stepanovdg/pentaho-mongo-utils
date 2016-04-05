/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package com.mongodb.connection;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * User: Dzmitry Stsiapanau Date: 03/31/2016 Time: 18:28
 */

@Aspect
public class ThreadSubjectAspect {

  @Around( "call(com.mongodb.connection.DefaultServerMonitor.new(..)) && args(serverId,serverSettings,"
    + "changeListener,connectionFactory,connectionPool)" )
  public DefaultServerMonitor aroundCallingMonitorRunnable2( ProceedingJoinPoint joinPoint, ServerId serverId,
                                                             ServerSettings serverSettings,
                                                             ChangeListener changeListener,
                                                             InternalConnectionFactory connectionFactory,
                                                             ConnectionPool connectionPool ) throws Throwable {
    return new DefaultServerMonitorWrapped( serverId, serverSettings, changeListener, connectionFactory,
      connectionPool );
  }

  private Runnable wrapACC( final Runnable monitor, final AccessControlContext context,
                            final ClassLoader contextClassLoader
  ) {
    return new Runnable() {
      public void run() {
        AccessController.doPrivileged( new PrivilegedAction<Void>() {
          public Void run() {
            Thread.currentThread().setContextClassLoader( contextClassLoader );
            monitor.run();
            return null;
          }
        }, context );
      }
    };
  }
}

