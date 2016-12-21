/**
 * CopyRight by Chinamobile
 *
 * TestUtil.java
 */

package com.chinamobile.bcbsp;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 *
 * TestUtil
 *
 */
public class TestUtil {

  /**
   * This method can help trace all the places where something is printed by the
   * user.
   *
   * @param args
   *        String
   */
  public void println(String args) {
    System .out .println(args);
  }

  /**
   * This method can help get the private attribute of object, but it needs to
   * cast the type of the result.
   *
   * @param object
   *        object from which the represented field's value is
   *        to be extracted
   * @param attribute
   *        the name of the field
   * @return
   *        get the object
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static Object get(Object object, String attribute) throws Exception {
    Class objectClass = object.getClass();
    Field field = objectClass.getDeclaredField(attribute);
    field.setAccessible(true);
    return field.get(object);
  }

  /**
   * This method can help set the private attribute of object the value.
   *
   * @param object
   *        the object whose field should be modified
   * @param attribute
   *        the name of the field
   * @param value
   *        the new value for the field of {@code obj}
   *        being modified
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void set(Object object, String attribute, Object value)
      throws Exception {
    Class objectClass = object.getClass();
    Field field = objectClass.getDeclaredField(attribute);
    field.setAccessible(true);
    field.set(object, value);
  }

  /**
   * This metho can help invoke the private method of the object, but it needs
   * to cast the type of the result.
   *
   * @param obj
   *        the object the underlying method is invoked from
   * @param methodName
   *        method name
   * @param args
   *        the arguments for the method to invoke
   * @return
   *        result
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static Object invoke(Object obj, String methodName, Object...args)
      throws Exception {

    Class[] types = new Class[args.length];
    Class tmp = null;
    for (int i = 0; i < types.length; i++) {
      tmp = args[i].getClass();
      if (Proxy.class.isAssignableFrom(tmp)) {
        if (tmp.getInterfaces() == null || tmp.getInterfaces().length == 0) {
          if (!Proxy.class.isAssignableFrom(tmp.getSuperclass())){
            tmp = tmp.getSuperclass();
          }
        } else {
          tmp = tmp.getInterfaces()[0];
        }
      }
      types[i] = tmp;
    }
    Method method = obj.getClass().getDeclaredMethod(methodName, types);
    method.setAccessible(true);

    Object result = null;
    result = method.invoke(obj, args);
    return result;
  }

  /**
   *
   * A class implements InvocationHandler
   *
   */
  public static class DonothingProxyHandler implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      return null;
    }
  }

  /**
   * Create a object of a interface
   *
   * @param theInterface
   *        Class<T>
   * @param <T>
   *        T
   * @return
   *       Donothing Object
   */
  public static <T> T createDonothingObject(Class<T> theInterface) {
    return theInterface.cast(Proxy.newProxyInstance(
        theInterface.getClassLoader(), new Class[] {theInterface},
        new DonothingProxyHandler()));
  }

}
