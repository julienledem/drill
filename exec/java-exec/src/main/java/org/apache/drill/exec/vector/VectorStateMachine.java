package org.apache.drill.exec.vector;

import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
/**
 * Rules for Vector states
 * - values need to be written in order (e.g. index 0, 1, 2, 5)
 * - you must call setValueCount before a vector can be read by another than that producer
 * - you should never write to a vector once it has been sealed by calling setValueCount.
 *
 */
public class VectorStateMachine {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorStateMachine.class);
  private static boolean failOnError = false;
  private static long baseId;

  public static void setFailOnError(boolean failOnError) {
    VectorStateMachine.failOnError = failOnError;
  }

  private static boolean initLog() {
    try {
      baseId = new Random().nextInt(1000);
      File path = new File("state-history");
      logger.info("state history at " + path.getAbsolutePath());
      history = new BufferedWriter(new FileWriter(path, true));
      history.write("START --------------------\n");
      history.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  private List<Transition> transitions = new ArrayList<>();

  private static Writer history;
  static {
    assert initLog();
  }
  private static int nextId = 0;

  private final int id = nextId++;
  private VectorState state = new Initial();

  private void log(Exception st) {
    log(st.getMessage(), st);
  }

  private void log(Transition t) {
    log(t.description, t.st);
  }

  private void log(String message, Exception st) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    st.printStackTrace(pw);
    pw.close();
    BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));
    try {
      log(message);
      br.readLine(); // skip unused message
      String l;
      while ((l = br.readLine()) != null) {
        if (l.contains("at org.junit.runners")) {
          break;
        }
        log(l);
      }
      flushLog();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void flushLog() throws IOException {
    history.flush();
  }

  private void log(String s) throws IOException {
    history.write(format("%,3d-%,4d: %s\n", baseId, id, s));
//    System.out.print(s);
  }

  private boolean transition(VectorState newState) {
    if (!state.isFailled()) {
      Transition newTransition = new Transition(state, newState);
      transitions.add(newTransition);
      if (newState.isFailled()) {
        if (failOnError) {
          throw new IllegalStateException(newTransition.toString());
        } else {
          for (Transition t : transitions) {
            log(t);
          }
        }
      }
    }
    state = newState;
    return true;
  }

  String name() {
    return state.name();
  }

  /**
   * called when we allocate memory before writing
   * @return true as it is meant to be called in an assert
   */
  public boolean allocateNew() {
    return transition(state.allocateNew());
  }

  /**
   * called when we're done writing
   * @param valueCount the total size of the vector
   * @return true as it is meant to be called in an assert
   */
  public boolean setValueCount(int valueCount) {
    return transition(state.setValueCount(valueCount));
  }

  /**
   * called when reading an index
   * @param index the index to read
   * @return true as it is meant to be called in an assert
   */
  public boolean read(int index) {
    return transition(state.read(index));
  }

  /**
   * called when writing to an index
   * @param index the index to write to
   * @return true as it is meant to be called in an assert
   */
  public boolean write(int index) {
    return transition(state.write(index));
  }

  /**
   * called when we are done reading and want to release resources
   * @return true as it is meant to be called in an assert
   */
  public boolean release() {
    return transition(state.release());
  }

  public boolean setInitialCapacity(int valueCount) {
    return transition(state.setInitialCapacity(valueCount));
  }

  public boolean reAlloc() {
    return transition(state.reAlloc());
  }

  public boolean zeroVector() {
    return transition(state.zeroVector());
  }

  public boolean load(int valueCount) {
    return transition(state.load(valueCount));
  }

  public boolean transfer() {
    return transition(state.transfer());
  }

  public boolean splitAndTransfer(int startIndex, int length) {
    return transition(state.splitAndTransfer(startIndex, length));
  }

  public boolean decrementAllocationMonitor() {
    return transition(state.decrementAllocationMonitor());
  }

  public boolean getValueCount() {
    return transition(state.getValueCount());
  }

  public boolean copy(int fromIndex, int toIndex) {
    return transition(state.copy(fromIndex, toIndex));
  }

  private abstract class VectorState {

    protected boolean isFailled() {
      return false;
    }

    protected VectorState fail() {
      return fail("disallowed transition");
    }

    protected VectorState fail(String message) {
      return new Failed(message);
    }

    protected VectorState checkRead(int index, int maxIndex, VectorState nextState) {
      if (index < 0) {
        return fail(format("read negative index %s < 0", index));
      }
      if (index > maxIndex) {
        return fail(format("read past max index written %s > %s", index, maxIndex));
      }
      return nextState;
    }

    protected VectorState checkWrite(int index, int maxIndex, VectorState nextState) {
      if (index < 0) {
        return fail(format("write negative index %s < 0", index));
      }
      if (index <= maxIndex) {
        return fail(format("write before last index written %s <= %s", index, maxIndex));
      }
      return nextState;
    }

    protected VectorState checkSetSize(int size, int maxIndex, VectorState nextState) {
      if (size < 0) {
        fail(format("write negative size %s < 0", size));
      }
      if (size <= maxIndex) {
        fail(format("set size smaller than last index written %s <= %s", size, maxIndex));
      }
      return nextState;
    }

    VectorState allocateNew() {
      return fail();
    }

    VectorState setValueCount(int valueCount) {
      return fail();
    }

    VectorState read(int index) {
      return fail();
    }

    VectorState write(int index) {
      return fail();
    }

    VectorState release() {
      return fail();
    }

    VectorState copy(int fromIndex, int toIndex) {
      return fail();
    }

    VectorState getValueCount() {
      return fail();
    }

    VectorState decrementAllocationMonitor() {
      return fail();
    }

    VectorState splitAndTransfer(int startIndex, int length) {
      return fail();
    }

    VectorState transfer() {
      return fail();
    }

    VectorState load(int valueCount) {
      return fail();
    }

    VectorState zeroVector() {
      return fail();
    }

    VectorState reAlloc() {
      return fail();
    }

    VectorState setInitialCapacity(int valueCount) {
      return fail();
    }

    String name() {
      return this.getClass().getSimpleName();
    }

    @Override
    public String toString() {
      return name();
    }
  }

  private class Initial extends VectorState {
    @Override
    VectorState allocateNew() {
      return new Writable();
    }

    @Override
    VectorState setInitialCapacity(int valueCount) {
      return this;
    }

    @Override
    VectorState setValueCount(int valueCount) {
      if (valueCount != 0) {
        return fail(format("cannot set non 0 value count (%d) for non allocated vector", valueCount));
      }
      return new ReadOnly(-1);
    }

    @Override
    VectorState load(int valueCount) {
      return new ReadOnly(valueCount - 1);
    }
  }

  private class Writable extends VectorState {

    private final int maxIndex;

    public Writable() {
      this(-1);
    }
    private Writable(int maxIndex) {
      this.maxIndex = maxIndex;
    }

    private VectorState updateMaxIndex(int index) {
      return checkWrite(index, maxIndex, new Writable(index));
    }

    @Override
    VectorState allocateNew() {
      if (maxIndex != -1) {
        return fail("can not re allocate non empty vector");
      }
      return new Writable();
    }

    @Override
    VectorState setValueCount(int valueCount) {
      return checkSetSize(valueCount, maxIndex, new ReadOnly(valueCount - 1));
    }

    @Override
    VectorState getValueCount() {
      return this;
    }

    @Override
    VectorState read(int index) {
      return checkRead(index, maxIndex, this);
    }

    @Override
    VectorState write(int index) {
      return updateMaxIndex(index);
    }

    @Override
    VectorState load(int valueCount) {
      if (maxIndex != -1) {
        return fail("can not load in non empty vector");
      }
      return new ReadOnly(valueCount - 1);
    }

    @Override
    VectorState zeroVector() {
      return this;
    }

    @Override
    public String toString() {
      return format("%s(%d)", name(), maxIndex);
    }
  }

  private class ReadOnly extends VectorState {

    private final int maxIndex;

    public ReadOnly(int maxIndex) {
      this.maxIndex = maxIndex;
    }

    @Override
    VectorState transfer() {
      return new Initial();
    }

    @Override // TODO: remove?
    VectorState splitAndTransfer(int startIndex, int length) {
      return checkRead(startIndex + length, maxIndex, this); // TODO: check target state
    }

    @Override
    VectorState getValueCount() {
      return this;
    }

    @Override
    VectorState read(int index) {
      return checkRead(index, maxIndex, this);
    }

    @Override
    VectorState release() {
      return new Initial();
    }

    @Override
    VectorState setValueCount(int valueCount) {
      if (valueCount != maxIndex + 1) {
        return fail("setValueCount " + valueCount + " on readonly state of maxIndex " + maxIndex);
      } else {
        // if the call has no effect we allow it but warn
        log(new Exception("calling setValueCount() again with no effect"));
        return this;
      }
    }

    @Override
    public String toString() {
      return format("%s(%d)", name(), maxIndex);
    }
  }
  /**
   * Once failed we just stay in this state
   */
  private class Failed extends VectorState {

    private final String message;

    Failed(String message) {
      super();
      this.message = message;
    }

    @Override
    public String toString() {
      return format("%s(\"%s\")", name(), message);
    }

    @Override
    protected boolean isFailled() {
      return true;
    }
  }

  private static class Transition {
    String description;
    Exception st = new Exception();

    public Transition(VectorState from, VectorState to) {
      super();
      Exception st = new Exception();
      // the calling method name
      String transition = st.getStackTrace()[2].getMethodName();
      this.description = from + " -(" + transition + ")-> " + to;
    }

    @Override
    public String toString() {
      return description;
    }

  }
}
