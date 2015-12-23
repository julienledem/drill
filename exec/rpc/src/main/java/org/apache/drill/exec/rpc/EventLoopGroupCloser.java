/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;

public class EventLoopGroupCloser {
  private static final Logger logger = getLogger(EventLoopGroupCloser.class);

  static class ClosingELG {
    final EventLoopGroup elg;
    final Future<?> status;
    final long closingStart;
    public ClosingELG(EventLoopGroup elg, Future<?> status, long closingStart) {
      super();
      this.elg = elg;
      this.status = status;
      this.closingStart = closingStart;
    }
  }

  private static Queue<EventLoopGroup> toClose = new LinkedBlockingQueue<>();
  private static List<ClosingELG> closing = new LinkedList<>();

  private static Runnable closer = new Runnable() {

    @Override
    public void run() {
      while (true) {
        if (toClose.isEmpty() && closing.isEmpty()) {
          try {
            // we don't want to spin doing nothing
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        EventLoopGroup next;
        while ((next = toClose.poll()) != null) {
          // this takes 1s to complete
          // known issue: https://github.com/netty/netty/issues/2545
          long timestamp = System.currentTimeMillis();
          Future<?> future = next.shutdownGracefully(0, 0, TimeUnit.SECONDS);
          closing.add(new ClosingELG(next, future, timestamp));
        }

        for (Iterator<ClosingELG> iterator = closing.iterator(); iterator.hasNext();) {
          ClosingELG nextELG = iterator.next();
          if (nextELG.status.isDone()) {
            long elapsed = System.currentTimeMillis() - nextELG.closingStart;
            if (elapsed > 500) {
              logger.info("closed eventLoopGroup " + nextELG.elg + " in " + elapsed + " ms");
            }
            iterator.remove();
          }
        }
      }
    }
  };

  static {
    new Thread(closer, "EventLoopGroup closer thread").start();
  }

  public static void eventuallyClose(EventLoopGroup elg) {
    toClose.add(elg);
  }
}
