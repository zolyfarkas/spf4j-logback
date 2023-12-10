package org.spf4j.log;

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.PackagingDataCalculator;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

@SuppressFBWarnings("EI_EXPOSE_REP")
public final class ThrowableProxy implements IThrowableProxy {

  private static final ThrowableProxy[] NO_SUPPRESSED = new ThrowableProxy[0];

  private final Throwable throwable;
  private final String className;
  private final String message;
  // package-private because of ThrowableProxyUtil
  private final StackTraceElementProxy[] stackTraceElementProxyArray;
  // package-private because of ThrowableProxyUtil
  private int commonFrames;
  private ThrowableProxy cause;
  private ThrowableProxy[] suppressed = NO_SUPPRESSED;
  private boolean isCyclic = false;
  private transient PackagingDataCalculator packagingDataCalculator;
  private boolean calculatedPackageData = false;

  public static ThrowableProxy create(final Throwable t) {
    return create(t, null, new HashMap<>(4));
  }

  private static ThrowableProxy create(final Throwable t, final ThrowableProxy parent,
          final Map<Throwable, ThrowableProxy> seen) {
    ThrowableProxy tp = seen.get(t);
    if (tp != null) {
      // TODO: I hope I interpret isCyclic correctly
      tp.isCyclic = true;
      return tp;
    }
    ThrowableProxy proxy = new ThrowableProxy(t, parent);
    seen.put(t, proxy);
    Throwable cause = t.getCause();
    if (cause != null) {
      proxy.setCause(create(cause, proxy, seen));
    }
    Throwable[] suppressed = t.getSuppressed();
    if (suppressed.length > 0) {
      ThrowableProxy[] nSuppressed = new ThrowableProxy[suppressed.length];
      for (int i = 0; i < suppressed.length; i++) {
        nSuppressed[i] = create(suppressed[i], proxy, seen);
      }
      proxy.setSuppressed(nSuppressed);
    }
    return proxy;
  }

  private ThrowableProxy(final Throwable throwable, @Nullable final ThrowableProxy parent) {

    this.throwable = throwable;
    this.className = throwable.getClass().getName();
    this.message = throwable.getMessage();
    this.stackTraceElementProxyArray = ThrowableProxyUtil.steArrayToStepArray(throwable.getStackTrace());
    if (parent != null) {
      this.commonFrames = ThrowableProxyUtil.findNumberOfCommonFrames(throwable.getStackTrace(),
              parent.stackTraceElementProxyArray);
    }
  }

  public static IThrowableProxy addSuppressed(final IThrowableProxy to, final IThrowableProxy supressedProxy) {
    return new IThrowableProxy() {
      @Override
      public String getMessage() {
        return to.getMessage();
      }

      @Override
      public String getClassName() {
        return to.getClassName();
      }

      @Override
      public StackTraceElementProxy[] getStackTraceElementProxyArray() {
        return to.getStackTraceElementProxyArray();
      }

      @Override
      public int getCommonFrames() {
        return to.getCommonFrames();
      }

      @Override
      public IThrowableProxy getCause() {
        return to.getCause();
      }

      @Override
      public IThrowableProxy[] getSuppressed() {
        IThrowableProxy[] supp = to.getSuppressed();
        IThrowableProxy[] nsuppressed = Arrays.copyOf(supp, supp.length + 1);
        nsuppressed[supp.length] = supressedProxy;
        return nsuppressed;
      }

      @Override
      public boolean isCyclic() {
        return to.isCyclic();
      }
    };
  }

  private void setCause(final ThrowableProxy tp) {
    this.cause = tp;
  }

  private void setSuppressed(final ThrowableProxy[] suppressed) {
    this.suppressed = suppressed;
  }

  public Throwable getThrowable() {
    return throwable;
  }

  public String getMessage() {
    return message;
  }

  /*
     * (non-Javadoc)
     *
     * @see ch.qos.logback.classic.spi.IThrowableProxy#getClassName()
   */
  public String getClassName() {
    return className;
  }

  public StackTraceElementProxy[] getStackTraceElementProxyArray() {
    return stackTraceElementProxyArray;
  }

  public int getCommonFrames() {
    return commonFrames;
  }

  /*
     * (non-Javadoc)
     *
     * @see ch.qos.logback.classic.spi.IThrowableProxy#getCause()
   */
  public IThrowableProxy getCause() {
    return cause;
  }

  public IThrowableProxy[] getSuppressed() {
    return suppressed;
  }

  public PackagingDataCalculator getPackagingDataCalculator() {
    // if original instance (non-deserialized), and packagingDataCalculator
    // is not already initialized, then create an instance.
    // here we assume that (throwable == null) for deserialized instances
    if (throwable != null && packagingDataCalculator == null) {
      packagingDataCalculator = new PackagingDataCalculator();
    }
    return packagingDataCalculator;
  }

  public void calculatePackagingData() {
    if (calculatedPackageData) {
      return;
    }
    PackagingDataCalculator pdc = this.getPackagingDataCalculator();
    if (pdc != null) {
      calculatedPackageData = true;
      pdc.calculate(this);
    }
  }

  @Override
  public String toString() {
    return "ThrowableProxy{" + "throwable=" + throwable + ", className=" + className + ", message=" + message + '}';
  }

  @Override
  public boolean isCyclic() {
    return this.isCyclic;
  }



}
