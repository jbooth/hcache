package hcache.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;

/** 
 * Re-usable buffer to hold writables, can produce representative
 * byte[]s for arbitrary writables.
 */
public class WritableBuffer<T extends Writable> extends BinaryComparable {
  private ByteBuffer buffer;
  private DataInput in;
  private DataOutput out;
  private Class<? extends T> currClass;
  
  /** Creates a new WritableBuffer with the given bufferSize.  WritableBuffers
   * will throw BufferOverflowExceptions if you try to put a writable in it
   * that exceeds its capacity.  */
  public WritableBuffer(int bufferSize) {
    this(ByteBuffer.allocate(bufferSize));
  }
  
  /** Wraps a WritableBuffer around the supplied array */
  public WritableBuffer(byte[] b) {
    this(ByteBuffer.wrap(b));
  }
  
  /** Wraps a WritableBuffer around the supplied ByteBuffer */
  public WritableBuffer(ByteBuffer buff) {
    buffer = buff;
    in = new DataInputStream(new ByteBufferInputStream(buffer));
    out = new DataOutputStream(new ByteBufferOutputStream(buffer));
    currClass=null;
  }
  
  /** 
   * Puts the item in the buffer.  If an item was previously in the buffer,
   * it is replaced.
   * 
   * @param item
   */
  @SuppressWarnings("unchecked")
  public void put(T item) throws IOException {
    // attempt to not overflow
    if (item instanceof BinaryComparable) {
      ensureCapacity(((BinaryComparable)item).getLength());
    }
    buffer.clear();
    item.write(out);
    buffer.flip();
    currClass = (Class<? extends T>)item.getClass();
  }
  
  /**
   * Fills the supplied writable with the current contents of the buffer.
   * @throws BufferUnderflowException if buffer is empty
   */
  public void get(T toFill) throws IOException {
    if (currClass == null) throw new IllegalStateException("No value in buffer!");
    buffer.mark();
    toFill.readFields(in);
    buffer.reset();
  }
  
  public Class<? extends T> getCurrentValueClass() {
    return currClass;
  }
  
  // BinaryComparable
  @Override
  public byte[] getBytes() {
    return buffer.array();
  }

  // BinaryComparable
  @Override
  public int getLength() {
    return buffer.remaining();
  }
  
  /**
   * If buffer.capacity() is lower than the supplied arguement, increases
   * it to capacity by allocating a new buffer.  Does not preserve
   * contents of buffer.
   * 
   * @param capacity
   */
  public void ensureCapacity(int capacity) {
    if (buffer.capacity() < capacity) {
      buffer = ByteBuffer.allocate(capacity);
    }
  }
  /** Utility, writes to a bytebuffer */
  public static class ByteBufferOutputStream extends OutputStream {
    private ByteBuffer buffer;

    /** Creates a ByteBufferOutputStream with the given backing buffer */
    public ByteBufferOutputStream(ByteBuffer buffer) {
      this.buffer = buffer;
    }
    
    /** @throws BufferOverflowException if buffer is full */
    @Override
    public void write(int b) throws IOException {
        buffer.put((byte) b);
    }

    /** @throws BufferOverflowException if buffer is full */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      buffer.put(b, off, len);
    }
  }
  
  /** Utility, reads from a bytebuffer */
  public static class ByteBufferInputStream extends InputStream {
    private ByteBuffer buffer;
    
    public ByteBufferInputStream(ByteBuffer buffer) {
      this.buffer=buffer;
    }

    @Override
    public int read() throws IOException {
      if (! buffer.hasRemaining()) return -1;
      return buffer.get() & 0xff;
    }
    
    @Override
    public int read(byte[] b, int off, int len) {
      if (len > buffer.remaining()) len = buffer.remaining();
      buffer.get(b, off, len);
      return len;
    }
  }
}
