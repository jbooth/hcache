package hcache.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Lightweight wrapper, takes existing FileChannel and ByteBuffer and provides
 * Seekable and DataInput.
 */
public class SeekableInputStream extends DataInputStream implements DataInput {
  private final PositionedInputStream myIn;

  public SeekableInputStream(FileChannel chan, long position, ByteBuffer buff)
      throws IOException {
    super(new PositionedInputStream(chan, position, buff));
    myIn = (PositionedInputStream) in;
  }

  public void seek(long pos) throws IOException {
    myIn.seek(pos);
  }
  
  public long getPos() {
    return myIn.getPos();
  }
  
  public void readFully(byte[] buf, int length) throws IOException {
    int numRead = 0;
    while (numRead < length) {
      int r = myIn.read(buf, numRead, length - numRead);
      if (r == -1) { throw new IOException("Returned EOF"); }
      numRead += r;
    }
  }

  /** Works via pread(), threadsafe on file */
  private static class PositionedInputStream extends InputStream {
    private final FileChannel chan;
    private final ByteBuffer buff;
    // pos we read from when refilling
    private long nextPos;
    private long currPos;

    public PositionedInputStream(FileChannel chan, long position,
        ByteBuffer buff) throws IOException {
      this.chan = chan;
      this.buff = buff;
      seek(position);
    }

    public long getPos() {
      return currPos;
    }
    @Override
    public int read() throws IOException {
      if (!buff.hasRemaining()) {
        int r = 0;
        while ((r = refill()) <= 0) {
          if (r < 0)
            return r;
        }
      }
      currPos++;
      // ensure positive
      return  ( ((int) buff.get()) & 0xff);
    }

    public void seek(long newPos) throws IOException {
      buff.clear();
      buff.limit(0);
      //if (newPos > fileSize) throw new EOFException("Seek past EOF");
      nextPos = newPos;
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b,0,b.length);
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (!buff.hasRemaining()) {
        int r = 0;
        while ((r = refill()) <= 0) {
          if (r < 0)
            return r;
        }
      }
      if (len > buff.remaining())
        len = buff.remaining();
      buff.get(b, off, len);
      currPos += len;
      return len;
    }

    private int refill() throws IOException {

      //if (nextPos > fileSize) throw new EOFException("Seek past EOF");
      buff.clear();
      int nRead = chan.read(buff, nextPos);
      // if none, return immediately
      if (nRead <= 0)
        return nRead;
      // got some bytes, adjust positions
      buff.flip();
      currPos = nextPos;
      // increment nextPos
      nextPos = nextPos + nRead;
      return nRead;
    }
  }
}
