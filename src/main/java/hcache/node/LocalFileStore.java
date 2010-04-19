package hcache.node;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Encapsulates details of storing versioned copies of files
 * 
 * All files are warehoused at fileStoreRoot
 */
public class LocalFileStore {
  private final File fileStoreRoot;
  private final FileSystem dfs;
  
  
  public LocalFileStore(Configuration conf) throws IOException {
    this(new File(conf.get("hcache.fileStoreRoot")),conf);
  }
  
  public LocalFileStore(File fileStoreRoot, Configuration conf) throws IOException {
    this.fileStoreRoot = fileStoreRoot;
    this.dfs = FileSystem.get(conf);
  }
  
  /** Returns the newest local CDB representing hadoopPath if it is 
   * newer than the file's last modified time in hadoop.  
   * 
   * Returns null otherwise. */
  public File checkFile(Path hadoopPath) { 
    File localPath = localPath(hadoopPath);
    
    if (! localPath.exists()) return null;
    
    File[] localCopies = localPath.listFiles();
    if (localCopies == null || localCopies.length == 0) return null;
    long newest = 0;
    String newestName=null;
    for (File f : localCopies) {
      String fname = f.getName();
      long fileStamp = Long.parseLong(fname.substring(fname.indexOf(".")));
      if (fileStamp > newest) newestName = fname;
    }
    if (newestName != null) return new File(localPath, newestName);
    else return null;
  }
  
  File getTmpDir() {
    File ret = new File(fileStoreRoot, "tmp");
    if (! ret.exists()) ret.mkdirs();
    return ret;
  }
  
  File commitLocal(Path hadoopPath, File cdbTmpFile) {
    File localPath = localPath(hadoopPath);
    File localCdb = new File(localPath, String.valueOf(System.currentTimeMillis() + ".cdb"));
    cdbTmpFile.renameTo(localCdb);
    return localCdb;
  }
  
  private File localPath(Path hadoopPath) {
    return new File(fileStoreRoot, hadoopPath.toString());
  }
  
}
