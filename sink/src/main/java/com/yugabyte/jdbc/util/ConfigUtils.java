/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.yugabyte.jdbc.util;

import java.util.Map;

/** Utilities for configuration properties. */
public class ConfigUtils {

  /**
   * Get the connector's name from the configuration.
   *
   * @param connectorProps the connector properties
   * @return the concatenated string with delimiters
   */
  public static String connectorName(Map<?, ?> connectorProps) {
    Object nameValue = connectorProps.get("name");
    return nameValue != null ? nameValue.toString() : null;
  }
}
