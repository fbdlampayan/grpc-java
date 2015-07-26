/*
 * Copyright 2015, Google Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *    * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.grpc;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;


/**
 * A context propagation mechanism which carries deadlines, cancellation signals,
 * and other scoped values across API boundaries and between threads. Examples of functionality
 * propagated via context include:
 * <ul>
 *   <li>Deadlines for a local operation or remote call.</li>
 *   <li>Security principals and credentials.</li>
 *   <li>Local and distributed tracing context.</li>
 * </ul>
 *
 * <p>Context objects make their state available by being attached to the executing thread using
 * a {@link ThreadLocal}. The context object bound to a thread is considered {@link #current()}.
 * Context objects are immutable and inherit state from their parent. To add or overwrite the
 * current state a new context object must be created and then attached to the thread replacing the
 * previously bound context. For example:
 * <pre>
 *   Context withCredential = Context.current().withValue(CRED_KEY, cred);
 *   executorService.execute(withCredential.wrap(new Runnable() {
 *     public void run() {
 *        readUserRecords(userId, CRED_KEY.get());
 *     }
 *   }));

 * </pre>
 *
 * <p>Context objects will cascade cancellation from their parent and propagate it to their
 * children. You can add a {@link CancellationListener} to a context to be notified when it or
 * one of its ancestors has been cancelled. Cancellation does not release the state stored by
 * a context and it's perfectly valid to {@link #attach()} an already cancelled context to a
 * thread to make it current. To cancel a context (and its descendants) you first create a
 * {@link CancellableContext} and when you need to signal cancellation call
 * {@link CancellableContext#cancel} or {@link CancellableContext#detachAndCancel}. For example:
 * <pre>
 *   CancellableContext withCancellation = Context.current().withCancellation();
 *   try {
   *   executorService.execute(withCancellation.wrap(new Runnable() {
 *       public void run() {
 *         while (waitingForData() &amp;&amp; !Context.current().isCancelled()) {}
 *       }
 *     });
 *     doSomeWork();
 *   } catch (Throwable t) {
 *      withCancellation.cancel(t);
 *   }
 * </pre>
 *
 *
 * <p>Notes and cautions on use:
 * <ul>
 *    <li>While Context objects are immutable they do not place such a restriction on the state
 * they store.</li>
 *    <li>Context is not intended for passing optional parameters to an API and developers should
 * take care to avoid excessive dependence on context when designing an API.</li>
 *    <li>If Context is being used in an environment that needs to support class unloading it is the
 * responsibility of the application to ensure that all contexts are properly cancelled.</li>
 * </ul>
 */
public abstract class Context {

  private static final Logger LOG = Logger.getLogger(Context.class.getName());

  /**
   * Use a shared resource to retain the {@link ScheduledExecutorService} used to
   * implement deadline based context cancellation. This allows the executor to be
   * shutdown if its not in use thereby allowing Context to be unloaded.
   */
  static final SharedResourceHolder.Resource<ScheduledExecutorService> SCHEDULER =
      new SharedResourceHolder.Resource<ScheduledExecutorService>() {
        private static final String name = "context-scheduler";
        @Override
        public ScheduledExecutorService create() {
          return Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
              .setNameFormat(name + "-%d")
              .setDaemon(true)
              .build());
        }

        @Override
        public void close(ScheduledExecutorService instance) {
          instance.shutdown();
        }

        @Override
        public String toString() {
          return name;
        }
      };

  /**
   * Stack of context objects which is used to record attach & detach history on a thread.
   */
  private static final ThreadLocal<ArrayDeque<Context>> contextStack =
      new ThreadLocal<ArrayDeque<Context>>() {
    @Override
    protected ArrayDeque<Context> initialValue() {
      return new ArrayDeque<Context>();
    }
  };

  /**
   * The logical root context which is {@link #current()} if no other context is bound. This context
   * is not cancellable and so will not cascade cancellation or retain listeners.
   */
  public static final Context ROOT = new RootContext();

  /**
   * Create a {@link Key} with the given name.
   */
  public static <T> Key<T> key(String name) {
    return new Key<T>(name);
  }

  /**
   * Create a {@link Key} with the given name and default value.
   */
  public static <T> Key<T> keyWithDefault(String name, T defaultValue) {
    return new Key<T>(name, defaultValue);
  }

  /**
   * Return the context associated with the current thread, will never return {@code null} as
   * the {@link #ROOT} context is implicitly associated with all threads.
   *
   * <p>Will never return {@link CancellableContext} even if one is attached, instead a
   * {@link Context} is returned with the same properties and lifetime. This is to avoid
   * code stealing the ability to cancel arbitrarily.
   */
  public static Context current() {
    ArrayDeque<Context> stack = contextStack.get();
    if (stack.isEmpty()) {
      return ROOT;
    }
    return stack.peekLast();
  }

  // Prevent initialization outside of this class.
  Context() {}

  /**
   * Create a new context which is independently cancellable and also cascades cancellation from
   * its parent. Callers should ensure that either {@link CancellableContext#cancel(Throwable)}
   * or {@link CancellableContext#detachAndCancel(Throwable)} are called to notify listeners and
   * release the resources associated with them.
   *
   * <p>Sample usage:
   * <pre>
   *   Context.CancellableContext withCancellation = Context.current().withCancellation();
   *   try {
   *     executorService.execute(withCancellation.wrap(new Runnable() {
   *       public void run() {
   *         Context current = Context.current();
   *         while (!current.isCancelled()) {
   *           keepWorking();
   *         }
   *       }
   *     });
   *     doSomethingRelatedWork();
   *   } catch (Throwable t) {
   *     withCancellation.cancel(t);
   *   }
   * </pre>
   */
  public CancellableContext withCancellation() {
    return new CancellableContext(this);
  }

  /**
   * Create a new context which will cancel itself after an absolute deadline expressed as
   * nanoseconds in the {@link System#nanoTime()} clock. The returned context will cascade
   * cancellation of its parent. Callers may explicitly cancel the returned context prior to
   * the deadline just as for {@link #withCancellation()},
   *
   * <p>It is recommended that callers only use this method when propagating a derivative of
   * a received existing deadline. When establishing a new deadline, {@link #withDeadlineAfter}
   * is the better mechanism.
   */
  public CancellableContext withDeadlineNanoTime(long deadlineNanoTime) {
    return withDeadlineAfter(deadlineNanoTime - System.nanoTime(), TimeUnit.NANOSECONDS);
  }

  /**
   * Create a new context which will cancel itself after the given {@code duration} from now.
   * The returned context will cascade cancellation of its parent. Callers may explicitly cancel
   * the returned context prior to the deadline just as for {@link #withCancellation()},
   *
   * <p>Sample usage:
   * <pre>
   *   Context.CancellableContext withDeadline = Context.current().withDeadlineAfter(5,
   *       TimeUnit.SECONDS);
   *   executorService.execute(withDeadline.wrap(new Runnable() {
   *     public void run() {
   *       Context current = Context.current();
   *       while (!current.isCancelled()) {
   *         keepWorking();
   *       }
   *     }
   *   });
   * </pre>
   */
  public CancellableContext withDeadlineAfter(long duration, TimeUnit unit) {
    Preconditions.checkArgument(duration >= 0, "duration must be greater than or equal to 0");
    Preconditions.checkNotNull(unit, "unit");
    return new CancellableContext(this, unit.toNanos(duration));
  }

  /**
   * Create a new context with the given key value set. The new context will cascade cancellation
   * from its parent.
   *
   <pre>
   *   Context withCredential = Context.current().withValue(CRED_KEY, cred);
   *   executorService.execute(withCredential.wrap(new Runnable() {
   *     public void run() {
   *        readUserRecords(userId, CRED_KEY.get());
   *     }
   *   }));
   * </pre>
   *
   */
  public <V> Context withValue(final Key<V> k1, final V v1) {
    Preconditions.checkNotNull(k1, "k1");
    return new ChildContext(this) {
      @Override
      Object lookup(Key<?> key) {
        if (key.equals(k1)) {
          return v1;
        }
        return super.lookup(key);
      }
    };
  }

  /**
   * Create a new context with the given key value set. The new context will cascade cancellation
   * from its parent.
   */
  public <V1, V2> Context withValues(final Key<V1> k1, final V1 v1, final Key<V2> k2, final V2 v2) {
    Preconditions.checkNotNull(k1, "k1");
    Preconditions.checkNotNull(k2, "k2");
    return new ChildContext(this) {
      @Override
      Object lookup(Key<?> key) {
        if (key.equals(k1)) {
          return v1;
        }
        if (key.equals(k2)) {
          return v2;
        }
        return super.lookup(key);
      }
    };
  }

  /**
   * Create a new context with the given key value set. The new context will cascade cancellation
   * from its parent.
   */
  public <V1, V2, V3> Context withValues(final Key<V1> k1, final V1 v1, final Key<V2> k2,
      final V2 v2, final Key<V3> k3, final V3 v3) {
    Preconditions.checkNotNull(k1, "k1");
    Preconditions.checkNotNull(k2, "k2");
    Preconditions.checkNotNull(k3, "k3");
    return new ChildContext(this) {
      @Override
      Object lookup(Key<?> key) {
        if (key.equals(k1)) {
          return v1;
        }
        if (key.equals(k2)) {
          return v2;
        }
        if (key.equals(k3)) {
          return v3;
        }
        return super.lookup(key);
      }
    };
  }

  /**
   * Create a new context which copies the values of this context but does not propagate its
   * cancellation.
   */
  public Context fork() {
    return new ChildContext(this) {
      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public Throwable cause() {
        return null;
      }

      @Override
      void addListener0(Context context, CancellationListener cancellationListener,
          Executor executor) {}

      @Override
      public void removeListener(CancellationListener cancellationListener) {}
    };
  }

  /**
   * Attach this context to the thread and make it {@link #current}, the previously current context
   * will be restored when detach is called. It is allowed to attach contexts where
   * {@link #isCancelled()} is {@code true}.
   */
  public void attach() {
    contextStack.get().addLast(this);
  }

  // Visible for testing
  boolean isCurrent() {
    return current() == this;
  }

  /**
   * Detach the current context from the thread and restore the context that was previously
   * attached to the thread as the 'current' context.
   *
   * @throws java.lang.IllegalStateException if this context is not {@link #current()}.
   */
  public void detach() {
    ArrayDeque<Context> stack = contextStack.get();
    if (stack.isEmpty()) {
      if (this == ROOT) {
        throw new IllegalStateException("Cannot detach root");
      } else {
        throw new IllegalStateException("Cannot detach non-root context when root is current");
      }
    }
    if (stack.peekLast() != this) {
      throw new IllegalStateException("Cannot detach a context that is not current");
    }
    stack.removeLast();
  }

  /**
   * Is this context cancelled.
   */
  public abstract boolean isCancelled();

  /**
   * If a context {@link #isCancelled()} then return the cause of the cancellation or
   * {@code null} if context was cancelled without a cause. If the context is not yet cancelled
   * will always return {@code null}.
   *
   * <p>The cause is provided for informational purposes only and implementations should generally
   * assume that it has already been handled and logged properly.
   */
  @Nullable
  public abstract Throwable cause();

  /**
   * Add a listener that will be notified when the context becomes cancelled.
   */
  public void addListener(CancellationListener cancellationListener, Executor executor) {
    Preconditions.checkNotNull(cancellationListener);
    Preconditions.checkNotNull(executor);
    addListener0(this, cancellationListener, executor);
  }

  abstract void addListener0(Context context, CancellationListener cancellationListener,
      Executor executor);

  /**
   * Remove a {@link CancellationListener}.
   */
  public abstract void removeListener(CancellationListener cancellationListener);

  /**
   * Wrap a {@link Runnable} so that it executes with this context as the {@link #current} context.
   */
  public Runnable wrap(final Runnable r) {
    return new Runnable() {
      @Override
      public void run() {
        attach();
        try {
          r.run();
        } finally {
          detach();
        }
      }
    };
  }

  /**
   * Wrap a {@link Callable} so that it executes with this context as the {@link #current} context.
   */
  public <C> Callable<C> wrap(final Callable<C> c) {
    return new Callable<C>() {
      @Override
      public C call() throws Exception {
        attach();
        try {
          return c.call();
        } finally {
          detach();
        }
      }
    };
  }

  /**
   * Lookup the value for a key in the context inheritance chain.
   */
  abstract Object lookup(Key<?> key);

  private static final class RootContext extends Context {
    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public Throwable cause() {
      return null;
    }

    @Override
    void addListener0(Context context, CancellationListener cancellationListener,
        Executor executor) {}

    @Override
    public void removeListener(CancellationListener cancellationListener) {}

    @Override
    Object lookup(Key<?> key) {
      return null;
    }
  }

  private static class ChildContext extends Context {
    private Context parent;

    ChildContext(Context parent) {
      this.parent = Preconditions.checkNotNull(parent);
    }

    @Override
    public boolean isCancelled() {
      return parent.isCancelled();
    }

    @Override
    public Throwable cause() {
      return parent.cause();
    }

    @Override
    void addListener0(Context context, CancellationListener cancellationListener,
        Executor executor) {
      parent.addListener0(context, cancellationListener, executor);
    }

    @Override
    public void removeListener(CancellationListener cancellationListener) {
      parent.removeListener(cancellationListener);
    }

    @Override
    Object lookup(Key<?> key) {
      return parent.lookup(key);
    }
  }

  /**
   * A context which inherits cancellation from its parent but which can also be independently
   * cancelled and which will propagate cancellation to its descendants.
   */
  public static final class CancellableContext extends ChildContext {

    private boolean cancelled;
    private Throwable cause;
    /**
     * A dummy that inherits from this to attach and detach so that you cannot retrieve a
     * cancellable context from Context.current().
     */
    private final Context dummy = new ChildContext(this);
    private ScheduledFuture<?> scheduledFuture;
    private ArrayList<ExecutableListener> listeners = new ArrayList<ExecutableListener>();
    private final CancellationListener parentListener = new CancellationListener() {
      @Override
      public void cancelled(Context context) {
        // Record cancellation with its cause.
        cancel(CancellableContext.super.cause());
      }
    };

    /**
     * Create a cancellable context that does not have a deadline.
     */
    private CancellableContext(Context parent) {
      super(parent);
      super.addListener(parentListener, MoreExecutors.directExecutor());
    }

    /**
     * Create a cancellable context that has a deadline.
     */
    private CancellableContext(Context parent, long delayNanos) {
      this(parent);
      final ScheduledExecutorService scheduler = SharedResourceHolder.get(SCHEDULER);
      scheduler.schedule(new Runnable() {
        @Override
        public void run() {
          try {
            cancel(new TimeoutException("context timed out"));
          } finally {
            SharedResourceHolder.release(SCHEDULER, scheduler);
          }
        }
      }, delayNanos, TimeUnit.NANOSECONDS);
    }


    @Override
    public void attach() {
      dummy.attach();
    }

    @Override
    public void detach() {
      dummy.detach();
    }

    @Override
    boolean isCurrent() {
      return dummy.isCurrent();
    }

    /**
     * Attach this context to the thread and return a {@link AutoCloseable} that can be
     * used with try-with-resource statements to properly {@link #detach} and {@link #cancel}
     * the context on completion.
     *
     * @return a {@link java.io.Closeable} which can be used with try-with-resource blocks.
     */
    public Closeable attachAsCloseable() {
      attach();
      return new Closeable() {
        @Override
        public void close() throws IOException {
          detachAndCancel(null);
        }
      };
    }

    /**
     * Cancel this context and optionally provide a cause for the cancellation. This
     * will trigger notification of listeners.
     *
     * @return {@code true} if this context cancelled the context and notified listeners,
     *    {@code false} if the context was already cancelled.
     */
    public boolean cancel(@Nullable Throwable cause) {
      boolean triggeredCancel = false;
      synchronized (this) {
        if (!cancelled) {
          cancelled = true;
          if (scheduledFuture != null) {
            // If we have a scheduled cancellation pending attempt to cancel it.
            scheduledFuture.cancel(false);
            scheduledFuture = null;
          }
          this.cause = cause;
          triggeredCancel = true;
        }
      }
      if (triggeredCancel) {
        notifyAndClearListeners();
      }
      return triggeredCancel;
    }

    /**
     * Cancel this context and detach it from the current context from the thread and restore the
     * context that was previously attached to the thread as the 'current' context.
     *
     * @throws java.lang.IllegalStateException if this context is not {@link #current()}.
     */
    public void detachAndCancel(@Nullable Throwable cause) {
      try {
        detach();
      } finally {
        cancel(cause);
      }
    }

    @Override
    public boolean isCancelled() {
      synchronized (this) {
        if (cancelled) {
          return true;
        }
      }
      // Parent may have been cancelled but the cancellation listener not yet run.
      return super.isCancelled();
    }

    @Nullable
    @Override
    public Throwable cause() {
      synchronized (this) {
        if (cancelled) {
          return cause;
        }
      }
      // Parent may have been cancelled but the cancellation listener not yet run.
      return super.cause();
    }

    @Override
    void addListener0(Context context, CancellationListener cancellationListener,
        Executor executor) {
      ExecutableListener executableListener =
          new ExecutableListener(context, executor, cancellationListener);
      synchronized (this) {
        if (isCancelled()) {
          executableListener.deliver();
        } else {
          listeners.add(executableListener);
        }
      }
    }

    /**
     * Notify all listeners that this context has been cancelled and immediately release
     * any reference to them so that they may be garbage collected.
     */
    private void notifyAndClearListeners() {
      ArrayList<ExecutableListener> tmpListeners;
      synchronized (this) {
        tmpListeners = listeners;
        listeners = null;
      }
      for (int i = 0; i < tmpListeners.size(); i++) {
        try {
          tmpListeners.get(i).deliver();
        } catch (Throwable t) {
          LOG.log(Level.INFO, "Exception notifying context listener", t);
        }
      }
      super.removeListener(parentListener);
    }

    // Used in tests to ensure that listeners are defined and released based on
    // cancellation propagation. It's very important to ensure that we do not
    // accidentally retain listeners.
    int listenerCount() {
      synchronized (this) {
        return listeners == null ? 0 : listeners.size();
      }
    }

    @Override
    public void removeListener(CancellationListener cancellationListener) {
      synchronized (this) {
        if (listeners == null) {
          return;
        }
        for (int i = listeners.size() - 1; i >= 0; i--) {
          if (listeners.get(i).listener == cancellationListener) {
            listeners.remove(i);
            // Just remove the first matching listener, given that we allow duplicate adds we should
            // allow for duplicates after remove.
            break;
          }
        }
      }
    }
  }

  /**
   * A listener notified on context cancellation.
   */
  public interface CancellationListener {
    /**
     * @param context the newly cancelled context.
     */
    public void cancelled(Context context);
  }

  /**
   * Key for indexing values stored in a context.
   */
  public static class Key<T> {

    private final String name;
    private final T defaultValue;

    Key(String name) {
      this(name, null);
    }

    Key(String name, T defaultValue) {
      this.name = Preconditions.checkNotNull(name);
      this.defaultValue = defaultValue;
    }

    /**
     * Get the value from the {@link #current()} context for this key.
     */
    @SuppressWarnings("unchecked")
    public T get() {
      return get(Context.current());
    }

    /**
     * Get the value from the specified context for this key.
     */
    @SuppressWarnings("unchecked")
    public T get(Context context) {
      T value = (T) context.lookup(this);
      return value == null ? defaultValue : value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Key<?> key = (Key<?>) o;

      return key.name.equals(this.name);
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }
  }

  /**
   * Stores original context, listener, and executor.
   */
  private static class ExecutableListener implements Runnable {
    private final Context context;
    private final Executor executor;
    private final CancellationListener listener;

    private ExecutableListener(Context context, Executor executor, CancellationListener listener) {
      this.context = context;
      this.executor = executor;
      this.listener = listener;
    }

    private void deliver() {
      executor.execute(this);
    }

    @Override
    public void run() {
      listener.cancelled(context);
    }
  }
}
