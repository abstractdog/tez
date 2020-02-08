package org.apache.tez.common;

import com.google.common.util.concurrent.MoreExecutors;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;

/**
 * A interoperability layer to work with multiple versions of guava.
 */
public class GuavaShim {

  private GuavaShim() {
  }

  private static Method executorMethod;

  public static Executor directExecutor() {
    Class klass = MoreExecutors.class;
    Executor executor = null;
    try {
      if (executorMethod != null) {
        return (Executor) executorMethod.invoke(null);
      }
      executorMethod = klass.getDeclaredMethod("directExecutor");
      executor = (Executor) executorMethod.invoke(null);
    } catch (NoSuchMethodException nsme) {
      try {
        executorMethod = klass.getDeclaredMethod("sameThreadExecutor");
        executor = (Executor) executorMethod.invoke(null);
      } catch (NoSuchMethodException nsmeSame) {
      } catch (IllegalAccessException iae) {
      } catch (InvocationTargetException ite) {
      }
    } catch (IllegalAccessException iae) {
    } catch (InvocationTargetException ite) {
    }
    return executor;
  }
}