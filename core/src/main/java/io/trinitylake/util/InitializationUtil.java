/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trinitylake.util;

import io.trinitylake.Initializable;
import io.trinitylake.relocated.com.google.common.base.Throwables;
import io.trinitylake.relocated.com.google.common.collect.Maps;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;

public class InitializationUtil {

  public static <T extends Initializable> T loadInitializable(
      String impl, Map<String, String> properties, Class<T> clazz) {
    ValidationUtil.checkNotNull(
        impl, "Cannot initialize an object without specifying an implementation");
    ValidationUtil.checkNotNull(
        properties, "Cannot initialize an object without specifying the class name");
    DynConstructor<T> ctor;
    try {
      ctor = DynConstructors.builder(clazz).impl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize class %s implementation %s: %s",
              clazz.getName(), impl, e.getMessage()),
          e);
    }

    T obj;
    try {
      obj = ctor.newInstance();

    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Catalog, %s does not implement Catalog.", impl), e);
    }

    obj.initialize(properties);
    return obj;
  }

  public static class DynConstructorBuilder {
    private final Class<?> baseClass;
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private DynConstructor<?> ctor = null;
    private final Map<String, Throwable> problems = Maps.newHashMap();

    public DynConstructorBuilder(Class<?> baseClass) {
      this.baseClass = baseClass;
    }

    public DynConstructorBuilder() {
      this.baseClass = null;
    }

    /**
     * Set the {@link ClassLoader} used to lookup classes by name.
     *
     * <p>If not set, the current thread's ClassLoader is used.
     *
     * @param newLoader a ClassLoader
     * @return this Builder for method chaining
     */
    public DynConstructorBuilder loader(ClassLoader newLoader) {
      this.loader = newLoader;
      return this;
    }

    public DynConstructorBuilder impl(String className, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className, true, loader);
        impl(targetClass, types);
      } catch (NoClassDefFoundError | ClassNotFoundException e) {
        // cannot load this implementation
        problems.put(className, e);
      }
      return this;
    }

    public <T> DynConstructorBuilder impl(Class<T> targetClass, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        ctor = new DynConstructor<>(targetClass.getConstructor(types), targetClass);
      } catch (NoSuchMethodException e) {
        // not the right implementation
        problems.put(methodName(targetClass, types), e);
      }
      return this;
    }

    @SuppressWarnings("unchecked")
    public <C> DynConstructor<C> buildChecked() throws NoSuchMethodException {
      if (ctor != null) {
        return (DynConstructor<C>) ctor;
      }
      throw buildCheckedException(baseClass, problems);
    }

    @SuppressWarnings("unchecked")
    public <C> DynConstructor<C> build() {
      if (ctor != null) {
        return (DynConstructor<C>) ctor;
      }
      throw buildRuntimeException(baseClass, problems);
    }
  }

  public static class DynConstructors {

    private DynConstructors() {}

    public static DynConstructorBuilder builder() {
      return new DynConstructorBuilder();
    }

    public static DynConstructorBuilder builder(Class<?> baseClass) {
      return new DynConstructorBuilder(baseClass);
    }
  }

  public static class DynConstructor<C> extends UnboundMethod {
    private final Constructor<C> ctor;
    private final Class<? extends C> constructed;

    private DynConstructor(Constructor<C> constructor, Class<? extends C> constructed) {
      super(null, "newInstance");
      this.ctor = constructor;
      this.constructed = constructed;
    }

    public C newInstanceChecked(Object... args) throws Exception {
      try {
        if (args.length > ctor.getParameterCount()) {
          return ctor.newInstance(Arrays.copyOfRange(args, 0, ctor.getParameterCount()));
        } else {
          return ctor.newInstance(args);
        }
      } catch (InstantiationException | IllegalAccessException e) {
        throw e;
      } catch (InvocationTargetException e) {
        Throwables.throwIfInstanceOf(e.getCause(), Exception.class);
        Throwables.throwIfInstanceOf(e.getCause(), RuntimeException.class);
        throw new RuntimeException(e.getCause());
      }
    }

    public C newInstance(Object... args) {
      try {
        return newInstanceChecked(args);
      } catch (Exception e) {
        Throwables.throwIfInstanceOf(e, RuntimeException.class);
        throw new RuntimeException(e.getCause());
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R invoke(Object target, Object... args) {
      ValidationUtil.checkArgument(
          target == null, "Invalid call to constructor: target must be null");
      return (R) newInstance(args);
    }

    /**
     * @deprecated since 1.7.0, visibility will be reduced in 1.8.0
     */
    @Deprecated // will become package-private
    @Override
    @SuppressWarnings("unchecked")
    public <R> R invokeChecked(Object target, Object... args) throws Exception {
      ValidationUtil.checkArgument(
          target == null, "Invalid call to constructor: target must be null");
      return (R) newInstanceChecked(args);
    }

    @Override
    public BoundMethod bind(Object receiver) {
      throw new IllegalStateException("Cannot bind constructors");
    }

    @Override
    public boolean isStatic() {
      return true;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(constructor=" + ctor + ", class=" + constructed + ")";
    }
  }

  private static class MakeConstructorAccessible implements PrivilegedAction<Void> {
    private final Constructor<?> hidden;

    MakeConstructorAccessible(Constructor<?> hidden) {
      this.hidden = hidden;
    }

    @Override
    public Void run() {
      hidden.setAccessible(true);
      return null;
    }
  }

  private static NoSuchMethodException buildCheckedException(
      Class<?> baseClass, Map<String, Throwable> problems) {
    NoSuchMethodException exc =
        new NoSuchMethodException(
            "Cannot find constructor for " + baseClass + "\n" + formatProblems(problems));
    problems.values().forEach(exc::addSuppressed);
    return exc;
  }

  private static RuntimeException buildRuntimeException(
      Class<?> baseClass, Map<String, Throwable> problems) {
    RuntimeException exc =
        new RuntimeException(
            "Cannot find constructor for " + baseClass + "\n" + formatProblems(problems));
    problems.values().forEach(exc::addSuppressed);
    return exc;
  }

  private static String formatProblems(Map<String, Throwable> problems) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, Throwable> problem : problems.entrySet()) {
      if (first) {
        first = false;
      } else {
        sb.append("\n");
      }
      sb.append("\tMissing ")
          .append(problem.getKey())
          .append(" [")
          .append(problem.getValue().getClass().getName())
          .append(": ")
          .append(problem.getValue().getMessage())
          .append("]");
    }
    return sb.toString();
  }

  private static String methodName(Class<?> targetClass, Class<?>... types) {
    StringBuilder sb = new StringBuilder();
    sb.append(targetClass.getName()).append("(");
    boolean first = true;
    for (Class<?> type : types) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(type.getName());
    }
    sb.append(")");
    return sb.toString();
  }

  public static class DynMethods {

    private DynMethods() {}

    /**
     * Constructs a new builder for calling methods dynamically.
     *
     * @param methodName name of the method the builder will locate
     * @return a Builder for finding a method
     */
    public static DynMethodBuilder builder(String methodName) {
      return new DynMethodBuilder(methodName);
    }
  }

  /**
   * Convenience wrapper class around {@link java.lang.reflect.Method}.
   *
   * <p>Allows callers to invoke the wrapped method with all Exceptions wrapped by RuntimeException,
   * or with a single Exception catch block.
   */
  public static class UnboundMethod {

    private final Method method;
    private final String name;
    private final int argLength;

    UnboundMethod(Method method, String name) {
      this.method = method;
      this.name = name;
      this.argLength =
          (method == null || method.isVarArgs()) ? -1 : method.getParameterTypes().length;
    }

    @SuppressWarnings("unchecked")
    <R> R invokeChecked(Object target, Object... args) throws Exception {
      try {
        if (argLength < 0) {
          return (R) method.invoke(target, args);
        } else {
          return (R) method.invoke(target, Arrays.copyOfRange(args, 0, argLength));
        }

      } catch (InvocationTargetException e) {
        Throwables.propagateIfInstanceOf(e.getCause(), Exception.class);
        Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
        throw Throwables.propagate(e.getCause());
      }
    }

    public <R> R invoke(Object target, Object... args) {
      try {
        return this.invokeChecked(target, args);
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, RuntimeException.class);
        throw Throwables.propagate(e);
      }
    }

    /**
     * Returns this method as a BoundMethod for the given receiver.
     *
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} for this method and the receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     */
    public BoundMethod bind(Object receiver) {
      ValidationUtil.checkState(
          !isStatic(), "Cannot bind static method %s", method.toGenericString());
      ValidationUtil.checkArgument(
          method.getDeclaringClass().isAssignableFrom(receiver.getClass()),
          "Cannot bind %s to instance of %s",
          method.toGenericString(),
          receiver.getClass());

      return new BoundMethod(this, receiver);
    }

    /** Returns whether the method is a static method. */
    public boolean isStatic() {
      return Modifier.isStatic(method.getModifiers());
    }

    /** Returns whether the method is a noop. */
    public boolean isNoop() {
      return this == NOOP;
    }

    /**
     * Returns this method as a StaticMethod.
     *
     * @return a {@link StaticMethod} for this method
     * @throws IllegalStateException if the method is not static
     */
    public StaticMethod asStatic() {
      ValidationUtil.checkState(isStatic(), "Method is not static");
      return new StaticMethod(this);
    }

    @Override
    public String toString() {
      return "UnboundMethod(name=" + name + " method=" + method.toGenericString() + ")";
    }

    /** Singleton {@link UnboundMethod}, performs no operation and returns null. */
    private static final UnboundMethod NOOP =
        new UnboundMethod(null, "NOOP") {
          /**
           * @deprecated since 1.7.0, visibility will be reduced in 1.8.0
           */
          @Deprecated // will become package-private
          @Override
          public <R> R invokeChecked(Object target, Object... args) {
            return null;
          }

          @Override
          public BoundMethod bind(Object receiver) {
            return new BoundMethod(this, receiver);
          }

          @Override
          public StaticMethod asStatic() {
            return new StaticMethod(this);
          }

          @Override
          public boolean isStatic() {
            return true;
          }

          @Override
          public String toString() {
            return "UnboundMethod(NOOP)";
          }
        };
  }

  public static class BoundMethod {
    private final UnboundMethod method;
    private final Object receiver;

    private BoundMethod(UnboundMethod method, Object receiver) {
      this.method = method;
      this.receiver = receiver;
    }

    public <R> R invokeChecked(Object... args) throws Exception {
      return method.invokeChecked(receiver, args);
    }

    public <R> R invoke(Object... args) {
      return method.invoke(receiver, args);
    }
  }

  public static class StaticMethod {
    private final UnboundMethod method;

    private StaticMethod(UnboundMethod method) {
      this.method = method;
    }

    public <R> R invokeChecked(Object... args) throws Exception {
      return method.invokeChecked(null, args);
    }

    public <R> R invoke(Object... args) {
      return method.invoke(null, args);
    }
  }

  public static class DynMethodBuilder {
    private final String name;
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private UnboundMethod method = null;

    public DynMethodBuilder(String methodName) {
      this.name = methodName;
    }

    /**
     * Set the {@link ClassLoader} used to lookup classes by name.
     *
     * <p>If not set, the current thread's ClassLoader is used.
     *
     * @param newLoader a ClassLoader
     * @return this Builder for method chaining
     */
    public DynMethodBuilder loader(ClassLoader newLoader) {
      this.loader = newLoader;
      return this;
    }

    /**
     * If no implementation has been found, adds a NOOP method.
     *
     * <p>Note: calls to impl will not match after this method is called!
     *
     * @return this Builder for method chaining
     */
    public DynMethodBuilder orNoop() {
      if (method == null) {
        this.method = UnboundMethod.NOOP;
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * @param className name of a class
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see java.lang.Class#forName(String)
     * @see java.lang.Class#getMethod(String, Class[])
     */
    public DynMethodBuilder impl(String className, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className, true, loader);
        impl(targetClass, methodName, argClasses);
      } catch (ClassNotFoundException e) {
        // not the right implementation
      }
      return this;
    }

    /**
     * Checks for an implementation, first finding the given class by name.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param className name of a class
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see java.lang.Class#forName(String)
     * @see java.lang.Class#getMethod(String, Class[])
     */
    public DynMethodBuilder impl(String className, Class<?>... argClasses) {
      impl(className, name, argClasses);
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * @param targetClass a class instance
     * @param methodName name of a method (different from constructor)
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see java.lang.Class#forName(String)
     * @see java.lang.Class#getMethod(String, Class[])
     */
    public DynMethodBuilder impl(Class<?> targetClass, String methodName, Class<?>... argClasses) {
      // don't do any work if an implementation has been found
      if (method != null) {
        return this;
      }

      try {
        this.method = new UnboundMethod(targetClass.getMethod(methodName, argClasses), name);
      } catch (NoSuchMethodException e) {
        // not the right implementation
      }
      return this;
    }

    /**
     * Checks for a method implementation.
     *
     * <p>The name passed to the constructor is the method name used.
     *
     * @param targetClass a class instance
     * @param argClasses argument classes for the method
     * @return this Builder for method chaining
     * @see java.lang.Class#forName(String)
     * @see java.lang.Class#getMethod(String, Class[])
     */
    public DynMethodBuilder impl(Class<?> targetClass, Class<?>... argClasses) {
      impl(targetClass, name, argClasses);
      return this;
    }

    /**
     * Returns the first valid implementation as a UnboundMethod or throws a RuntimeError if there
     * is none.
     *
     * @return a {@link UnboundMethod} with a valid implementation
     * @throws RuntimeException if no implementation was found
     */
    public UnboundMethod build() {
      if (method != null) {
        return method;
      } else {
        throw new RuntimeException("Cannot find method: " + name);
      }
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a RuntimeError if there is
     * none.
     *
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} with a valid implementation and receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws RuntimeException if no implementation was found
     */
    public BoundMethod build(Object receiver) {
      return build().bind(receiver);
    }

    /**
     * Returns the first valid implementation as a UnboundMethod or throws a NoSuchMethodException
     * if there is none.
     *
     * @return a {@link UnboundMethod} with a valid implementation
     * @throws NoSuchMethodException if no implementation was found
     */
    public UnboundMethod buildChecked() throws NoSuchMethodException {
      if (method != null) {
        return method;
      } else {
        throw new NoSuchMethodException("Cannot find method: " + name);
      }
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a NoSuchMethodException if
     * there is none.
     *
     * @param receiver an Object to receive the method invocation
     * @return a {@link BoundMethod} with a valid implementation and receiver
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws NoSuchMethodException if no implementation was found
     */
    public BoundMethod buildChecked(Object receiver) throws NoSuchMethodException {
      return buildChecked().bind(receiver);
    }

    /**
     * Returns the first valid implementation as a StaticMethod or throws a NoSuchMethodException if
     * there is none.
     *
     * @return a {@link StaticMethod} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws NoSuchMethodException if no implementation was found
     */
    public StaticMethod buildStaticChecked() throws NoSuchMethodException {
      return buildChecked().asStatic();
    }

    /**
     * Returns the first valid implementation as a StaticMethod or throws a RuntimeException if
     * there is none.
     *
     * @return a {@link StaticMethod} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws RuntimeException if no implementation was found
     */
    public StaticMethod buildStatic() {
      return build().asStatic();
    }
  }

  private static class MakeMethodAccessible implements PrivilegedAction<Void> {
    private final Method hidden;

    MakeMethodAccessible(Method hidden) {
      this.hidden = hidden;
    }

    @Override
    public Void run() {
      hidden.setAccessible(true);
      return null;
    }
  }
}
