/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.skiplist.exception;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class HeapFullException extends CCSMapException {

  private static final long serialVersionUID = -2473741608268169616L;

  public HeapFullException() {
    super();
  }

  public HeapFullException(String message) {
    super(message);
  }

  public HeapFullException(String message, Throwable cause) {
    super(message, cause);
  }

  public HeapFullException(Throwable cause) {
    super(cause);
  }
}
