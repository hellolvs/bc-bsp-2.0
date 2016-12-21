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

package com.chinamobile.bcbsp.util;

import com.chinamobile.bcbsp.comm.BSPMessage;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//a reference: 4 bytes
//an Object: 8 bytes
//an Integer: 16 bytes == (8 + 4) / 8 * 8
//an int: 4 bytes
//size of array with zero elements: JRo64 = 24, Sun32 = 12
//size of reference，such as Object = null: Sun32 = 4, Sun64 = 8
//size of object without elements，such as new Object();: Sun32 = 8, Sun64 = 16
//size of byte[0]: Sun32 = 8 + 4, Sun64 = 16 + 8
//size of byte[l]: (l + 19) / 8 * 8
//size of char[l]/short[l]: (l * 2 + 19) / 8 * 8 == (l + 9) / 4 * 8
//size of String with l elements: (l + 1) * 2 + 32
//size of int[l]: (l * 4 + 19) / 8 * 8 == (l + 4) / 2 * 8
//size of long[l]: (l * 8 + 19) / 8 * 8 == (l + 2) * 8
/**
 * ObjectSizer
 */
public class ObjectSizer {
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(ObjectSizer.class);
  /** Define the size of the null reference */
  private final byte nullReferenceSize;
  /** Define the size of the object */
  private final byte emptyObjectSize;
  /** Define the size of the array */
  private final byte emptyArrayVarSize;
  /** Define a ArrayList */
  private List dedup = new ArrayList();

  /**
   * Construct
   *
   * @param nullReferenceSize
   *        the size of the reference
   * @param emptyObjectSize
   *        the size of the object
   * @param emptyArrayVarSize
   *        the size of the array
   */
  public ObjectSizer(byte nullReferenceSize, byte emptyObjectSize,
      byte emptyArrayVarSize) {
    this.nullReferenceSize = nullReferenceSize;
    this.emptyObjectSize = emptyObjectSize;
    this.emptyArrayVarSize = emptyArrayVarSize;
  }

  /**
   * The size of an object on 32-bit virtual machine.
   *
   * @return The size of an object.
   */
  public static ObjectSizer forSun32BitsVM() {
    return new ObjectSizer((byte) 4, (byte) 8, (byte) 4);
  }

  /**
   * The size of an object on 64-bit virtual machine.
   *
   * @return The size of an object.
   */
  public static ObjectSizer forSun64BitsVM() {
    return new ObjectSizer((byte) 8, (byte) 16, (byte) 8);
  }

  /**
   * The size of BSPMessage.
   *未被调用
   * @param msg BSPMessage
   * @return The size of BSPMessage.
   */
  public int sizeOf(BSPMessage msg) {
    int size = this.emptyObjectSize;
    size = size + sizeofPrimitiveClass(int.class); // int of dstPartition
    size = size + 2 * msg.getDstVertexID().length() + 32; // String of dstVertex
    if (msg.getData() != null) {
      size = size + msg.getData().length + 19; // byte[] of data
    }
    if (msg.getTag() != null) {
      size = size + msg.getTag().length + 19; // byte[] of tag
    }
    return size;
  }

  /**
   * The size of a reference.
   *
   * @return The size of a reference.
   */
  public int sizeOfRef() {
    return this.nullReferenceSize;
  }

  /**
   * The size of a char.
   *
   * @return The size of a char.
   */
  public int sizeOfChar() {
    return sizeofPrimitiveClass(char.class);
  }

  /**
   * The size of an object.
   *
   * @param object An object
   * @return The size of an object.
   */
  public int sizeOf(Object object) {
    dedup.clear();
    return calculate(object);
  }

  /**
   * Reference
   */
  private static class Ref {
    /** Define an object */
    private final Object obj;

    /**
     * Construct
     *
     * @param obj An object
     */
    public Ref(Object obj) {
      this.obj = obj;
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof Ref) && ((Ref) obj).obj == this.obj;
    }

    @Override
    public int hashCode() {
      return obj.hashCode();
    }
  }

  /**
   * Calculate the size of the object takes up space.
   * This object is an array or object.
   *
   * @param object An object
   * @return The size of the object takes up space
   */
  @SuppressWarnings("unchecked")
  private int calculate(Object object) {
    if (object == null) {
      return 0;
    }
    Ref r = new Ref(object);
    if (dedup.contains(r)) {
      return 0;
    }
    dedup.add(r);
    int varSize = 0;
    int objSize = 0;
    for (Class clazz = object.getClass(); clazz != Object.class; clazz = clazz
        .getSuperclass()) {
      if (clazz.isArray()) {
        varSize += emptyArrayVarSize;
        Class<?> componentType = clazz.getComponentType();
        if (componentType.isPrimitive()) {
          varSize += lengthOfPrimitiveArray(object) *
              sizeofPrimitiveClass(componentType);
          return occupationSize(emptyObjectSize, varSize, 0);
        }

        Object[] array = (Object[]) object;
        varSize += nullReferenceSize * array.length;
        for (Object o : array) {
          objSize += calculate(o);
        }
        return occupationSize(emptyObjectSize, varSize, objSize);
      }

      Field[] fields = clazz.getDeclaredFields();
      for (Field field : fields) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        if (clazz != field.getDeclaringClass()) {
          continue;
        }
        Class<?> type = field.getType();
        if (type.isPrimitive()) {
          varSize += sizeofPrimitiveClass(type);
        } else {
          varSize += nullReferenceSize;
          try {
            field.setAccessible(true);
            objSize += calculate(field.get(object));
          } catch (Exception e) {
            LOG.error("[calculate]", e);
            objSize += occupyofConstructor(object, field);
          }
        }
      }
    }
    return occupationSize(emptyObjectSize, varSize, objSize);
  }

  /**
   * Exception handling
   *
   * @param object An object
   * @param field Object class field
   * @return exception
   */
  private static int occupyofConstructor(Object object, Field field) {
    throw new UnsupportedOperationException(
        "field type Constructor not accessible: " + object.getClass()  +
        " field:" + field);
  }

  /**
   * Calculate the size of the object takes up space.
   *
   * @param size A size
   * @return Take up the space size.
   */
  private static int occupationSize(int size) {
    return (size + 7) / 8 * 8;
  }

  /**
   * Calculate the size of the object takes up space.
   *
   * @param selfSize Size of the object itself
   * @param varsSize Vars size
   * @param objsSize Size of an object
   * @return Take up the space size.
   */
  private static int occupationSize(int selfSize, int varsSize, int objsSize) {
    return occupationSize(selfSize) + occupationSize(varsSize) + objsSize;
  }

  /**
   * @param clazz data types
   * @return The byte size of different data types
   */
  private static int sizeofPrimitiveClass(Class clazz) {
    return clazz == boolean.class || clazz == byte.class ? 1 :
      clazz == char.class || clazz == short.class ? 2 : clazz == int.class ||
      clazz == float.class ? 4 : 8;
  }

  /**
   * @param object An object
   * @return The byte size of different object
   */
  private static int lengthOfPrimitiveArray(Object object) {
    Class<?> clazz = object.getClass();
    return clazz == boolean[].class ? ((boolean[]) object).length :
      clazz == byte[].class ? ((byte[]) object).length :
        clazz == char[].class ? ((char[]) object).length :
          clazz == short[].class ? ((short[]) object).length :
            clazz == int[].class ? ((int[]) object).length :
              clazz == float[].class ? ((float[]) object).length :
                clazz == long[].class ? ((long[]) object).length :
                  ((double[]) object).length;
  }

  /** For JUnit test. */
  public byte getEmptyArrayVarSize() {
    return emptyArrayVarSize;
  }
}
