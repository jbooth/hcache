package hcache.node;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Encapsulates details of storing versioned copies of files
 * 
 * All files are warehoused at fileStoreRoot, and are downloaded
 * into files mirroring their paths from root on hadoop.  Inside the final
 * directory, several timestamped versions may be stored.  
 * 
 * TODO cleanup logic
 */
public class LocalFileStore {
  private final File fileStoreRoot;
  private final FileSystem hadoop;
  public static final Log LOG = LogFactory.getLog(LocalFileStore.class);
  
  
  public LocalFileStore(Configuration conf) throws IOException {
    this(new File(conf.get("hcache.fileStoreRoot","/tmp/hcache")),conf);
  }
  
  public LocalFileStore(File fileStoreRoot, Configuration conf) throws IOException {
    this.fileStoreRoot = fileStoreRoot;
    this.hadoop = FileSystem.get(conf);
  }
  
  /** Returns the newest local CDB representing hadoopPath if it is 
   * newer than the file's last modified time in hadoop.  
   * 
   * Returns null otherwise. */
  public File checkFile(Path hadoopPath) throws IOException { 
    File localPath = localPath(hadoopPath);
    LOG.info("Finding " + hadoopPath + ", local root is " + localPath);
    if (! localPath.exists()) return null;
    
    File[] localCopies = localPath.listFiles();
    if (localCopies == null || localCopies.length == 0) return null;
    long newest = hadoop.getFileStatus(hadoopPath).getModificationTime();
    LOG.info("Need newer than newest : " + newest);
    String newestName=null;
    for (File f : localCopies) {
      String fname = f.getName();
      LOG.info(fname);
      
      long fileStamp = Long.parseLong(fname.substring(0, fname.indexOf(".")));

      LOG.info(fname + " : " + fileStamp);
      if (fileStamp > newest) newestName = fname;
    }
    if (newestName != null) return new File(localPath, newestName);
    else return null;
  }
  
  /**
   * Commits a CDB built by HCacheNode from it's temporary location
   * to the 'real' location using renameTo.
   * 
   * @param hadoopPath
   * @param cdbTmpFile
   * @return
   */
  File commitLocal(Path hadoopPath, File cdbTmpFile) {
    File localPath = localPath(hadoopPath);
    if (! localPath.exists()) localPath.mkdirs();
    File localCdb = new File(localPath, String.valueOf(System.currentTimeMillis() + ".cdb"));
    LOG.info("Renaming " + cdbTmpFile + " to " + localCdb);
    cdbTmpFile.renameTo(localCdb);
    return localCdb;
  }
  
  /**
   * Returns a new temporary file for streaming, guaranteed to be unique and in
   * a subdirectory of the main hcache root, enabling atomic mv to the main store
   * as long as it's the same filesystem.  Intended for use by HCacheNode.
   */
  synchronized File createTmp() {
    File tmp = new File(fileStoreRoot, "tmp");
    if (! tmp.exists()) tmp.mkdirs();
    File ret = new File(tmp, UUID.randomUUID().toString());
    while (ret.exists()) {
      ret = new File(tmp, UUID.randomUUID().toString());
    }
    return ret;
  }
  
  private File localPath(Path hadoopPath) {
    return new File(fileStoreRoot, hadoopPath.toString());
  }
  
}
