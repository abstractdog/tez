/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.common;

import javax.annotation.Nullable;

/**
 * A wrapper class for guava's Preconditions for making it easy to handle its usage in Tez project.
 */
public class Preconditions {

  public static void checkArgument(boolean expression) {
    com.google.common.base.Preconditions.checkArgument(expression);
  }

  public static void checkArgument(boolean expression, @Nullable Object errorMessage) {
    com.google.common.base.Preconditions.checkArgument(expression, errorMessage);
  }

  public static void checkArgument(boolean expression, @Nullable String errorMessageTemplate,
      @Nullable Object... errorMessageArgs) {
    if (!expression) { // prevent unnecessary preformatting here
      com.google.common.base.Preconditions.checkArgument(expression,
          lenientFormat(errorMessageTemplate, errorMessageArgs));
    }
  }

  public static void checkState(boolean expression) {
    com.google.common.base.Preconditions.checkState(expression);
  }

  public static void checkState(boolean expression, @Nullable Object errorMessage) {
    com.google.common.base.Preconditions.checkState(expression, errorMessage);
  }

  public static void checkState(boolean expression, @Nullable String errorMessageTemplate,
      @Nullable Object... errorMessageArgs) {
    if (!expression) { // prevent unnecessary preformatting here
      com.google.common.base.Preconditions.checkState(expression,
          lenientFormat(errorMessageTemplate, errorMessageArgs));
    }
  }

  private static String lenientFormat(@Nullable String template, @Nullable Object... args) {
    template = String.valueOf(template); // null -> "null"

    if (args == null) {
      args = new Object[] { "(Object[])null" };
    } else {
      for (int i = 0; i < args.length; i++) {
        args[i] = lenientToString(args[i]);
      }
    }

    // start substituting the arguments into the '%s' placeholders
    StringBuilder builder = new StringBuilder(template.length() + 16 * args.length);
    int templateStart = 0;
    int i = 0;
    while (i < args.length) {
      int placeholderStart = template.indexOf("%s", templateStart);
      if (placeholderStart == -1) {
        break;
      }
      builder.append(template, templateStart, placeholderStart);
      builder.append(args[i++]);
      templateStart = placeholderStart + 2;
    }
    builder.append(template, templateStart, template.length());

    // if we run out of placeholders, append the extra args in square braces
    if (i < args.length) {
      builder.append(" [");
      builder.append(args[i++]);
      while (i < args.length) {
        builder.append(", ");
        builder.append(args[i++]);
      }
      builder.append(']');
    }

    return builder.toString();
  }

  private static String lenientToString(@Nullable Object o) {
    try {
      return String.valueOf(o);
    } catch (Exception e) {
      String objectToString = o.getClass().getName() + '@' + Integer.toHexString(System.identityHashCode(o));
      return "<" + objectToString + " threw " + e.getClass().getName() + ">";
    }
  }
}
