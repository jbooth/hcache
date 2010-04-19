/*
 * Copyright (c) 2000-2001, Michael Alyn Miller <malyn@strangeGizmo.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice unmodified, this list of conditions, and the following
 *    disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of Michael Alyn Miller nor the names of the
 *    contributors to this software may be used to endorse or promote
 *    products derived from this software without specific prior written
 *    permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package hcache.io;

/* Java imports. */
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

/**
 * Modified version of CDB format, omitting lengths for tighter
 * record packing and moving header section from the first 2k 
 * to the last 2k.
 *
 * Original author Michael Alyn Miller <malyn@strangeGizmo.com>
 */
public final class CdbWriter<K extends Writable,V extends Writable> {
	/** The RandomAccessFile for the CDB file. */
	private FSDataOutputStream file_ = null;

	/** The list of hash pointers in the file, in their order in the
	 * constant database. */
	private Vector hashPointers_ = null;

	/** The number of entries in each hash table. */
	private int[] tableCount_ = null;

	/** The first entry in each table. */
	private int[] tableStart_ = null;

	/** The position of the current key in the constant database. */
	private int pos_ = 0;

	/** Writes a local Cdb file */
	public CdbWriter(File file, int bufferSize) throws IOException {
    this(
        new BufferedOutputStream(new FileOutputStream(file), bufferSize));
	}

	/**
	 * Opens a CDB file on the FileSystem denoted by conf
	 * 
	 * @param filepath The path to the constant database to create.
	 * @exception java.io.IOException If an error occurs creating the
	 *  constant database file.
	 */
	public CdbWriter(Path filepath, Configuration conf) throws IOException {
		this(FileSystem.get(conf).create(filepath));
	}
	
	public CdbWriter(OutputStream out) throws IOException {
    /* Open the temporary CDB file. */
	  this.file_ = new FSDataOutputStream(out);
    /* Initialize the class. */
    hashPointers_ = new Vector();
    tableCount_ = new int[256];
    tableStart_ = new int[256];

    /* Clear the table counts. */
    for (int i = 0; i < 256; i++)
      tableCount_[i] = 0;
    /* Records can't be at position zero, so write one filler byte */
    out.write((byte)-1);
    posplus(1);
	}

	/**
	 * Adds a key to the constant database.
	 *
	 * @param key The key to add to the database.
	 * @param data The data associated with this key.
	 * @exception java.io.IOException If an error occurs adding the key
	 *  to the database.
	 */
	// TODO remove lengths from format
	public void add(K key, V data) throws IOException {
	  
	  int recordPos = (int)file_.getPos();
	  /* Write out the key and data . */
		key.write(file_);
		data.write(file_);
		int recordLen = ((int)file_.getPos() - recordPos);
		
		/* Add the hash pointer to our list. */
		int hash = key.hashCode();
		hashPointers_.addElement(new CdbHashPointer(hash, pos_));
		/* Add this item to the count. */
		tableCount_[hash & 0xff]++;
		
		/* Update the file position pointer. */
		posplus(recordLen);
	}

	/**
	 * Finalizes the constant database.
	 *
	 * @exception java.io.IOException If an error occurs closing out the
	 *  database.
	 */
	public void close() throws IOException {
		/* Find the start of each hash table. */
		int curEntry = 0;
		for (int i = 0; i < 256; i++) {
			curEntry += tableCount_[i];
			tableStart_[i] = curEntry;
		}

		/* Create a new hash pointer list in order by hash table. */
		CdbHashPointer[] slotPointers
			= new CdbHashPointer[hashPointers_.size()];
		for (Enumeration e = hashPointers_.elements(); e.hasMoreElements(); ) {
			CdbHashPointer hp = (CdbHashPointer)e.nextElement();
			slotPointers[--tableStart_[hp.hash & 0xff]] = hp;
		}

		/* Write out each of the hash tables, building the slot table in
		 * the process. */
		byte[] slotTable = new byte[2048];
		for (int i = 0; i < 256; i++) {
			/* Get the length of the hashtable. */
			int len = tableCount_[i] * 2;

			/* Store the position of this table in the slot table. */
			slotTable[(i * 8) + 0] = (byte)(pos_ & 0xff);
			slotTable[(i * 8) + 1] = (byte)((pos_ >>>  8) & 0xff);
			slotTable[(i * 8) + 2] = (byte)((pos_ >>> 16) & 0xff);
			slotTable[(i * 8) + 3] = (byte)((pos_ >>> 24) & 0xff);
			slotTable[(i * 8) + 4 + 0] = (byte)(len & 0xff);
			slotTable[(i * 8) + 4 + 1] = (byte)((len >>>  8) & 0xff);
			slotTable[(i * 8) + 4 + 2] = (byte)((len >>> 16) & 0xff);
			slotTable[(i * 8) + 4 + 3] = (byte)((len >>> 24) & 0xff);

			/* Build the hash table. */
			int curSlotPointer = tableStart_[i];
			CdbHashPointer hashTable[] = new CdbHashPointer[len];
			for (int u = 0; u < tableCount_[i]; u++) {
				/* Get the hash pointer. */
				CdbHashPointer hp = slotPointers[curSlotPointer++];

				/* Locate a free space in the hash table. */
				int where = (hp.hash >>> 8) % len;
				while (hashTable[where] != null)
					if (++where == len)
						where = 0;

				/* Store the hash pointer. */
				hashTable[where] = hp;
			}

			/* Write out the hash table. */
			for (int u = 0; u < len; u++ ) {
				CdbHashPointer hp = hashTable[u];
				if (hp != null) {
					writeLeInt(hashTable[u].hash);
					writeLeInt(hashTable[u].pos);
				} else {
					writeLeInt(0);
					writeLeInt(0);
				}
				posplus(8);
			}
		}

		/* Append the slot table as the last 2kb of file. */
		file_.write(slotTable);

		/* Close the file. */
		file_.close();
	}


	/**
	 * Writes an integer in little-endian format to the constant
	 * database at the current file offset.
	 *
	 * @param v The integer to write to the file.
	 */
	private void writeLeInt(int v) throws IOException {
		file_.writeByte((byte)(v & 0xff));
		file_.writeByte((byte)((v >>>  8) & 0xff));
		file_.writeByte((byte)((v >>> 16) & 0xff));
		file_.writeByte((byte)((v >>> 24) & 0xff));
	}

	/**
	 * Advances the file pointer by <code>count</code> bytes, throwing
	 * an exception if doing so would cause the file to grow beyond
	 * 4 GB.
	 *
	 * @param count The count of bytes to increase the file pointer by.
	 * @exception java.io.IOException If increasing the file pointer by
	 *  <code>count</code> bytes would cause the file to grow beyond
	 *  4 GB.
	 */
	private void posplus(int count) throws IOException {
		int newpos = pos_ + count;
		if (newpos < count)
			throw new IOException("CDB file is too big to add " + count + " more bytes. Current pos: " + pos_);
		pos_ = newpos;
	}
}


class CdbHashPointer {
	/** The hash value of this entry. */
	final int hash;

	/** The position in the constant database of this entry. */
	final int pos;


	/**
	 * Creates a new CdbHashPointer and initializes it with the given
	 * hash value and position.
	 *
	 * @param hash The hash value for this hash pointer.
	 * @param pos The position of this entry in the constant database.
	 */
	CdbHashPointer(int hash, int pos) {
		this.hash = hash;
		this.pos = pos;
	}
}
