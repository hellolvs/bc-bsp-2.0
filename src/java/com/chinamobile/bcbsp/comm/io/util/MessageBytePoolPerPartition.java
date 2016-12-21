
package com.chinamobile.bcbsp.comm.io.util;

import com.chinamobile.bcbsp.comm.BSPMessagesPack;
import com.chinamobile.bcbsp.comm.CommunicationFactory;
import com.chinamobile.bcbsp.comm.IMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Manager Message Data In Bytes For Efficient Processing And Storage.
 * @author Liu Jinpeng
 */
public class MessageBytePoolPerPartition implements WritableBSPMessages {
  /** Class logger. */
  private static final Log LOG = LogFactory
      .getLog(MessageBytePoolPerPartition.class);
  /** handle of byte array pool. */
  private ExtendedDataOutput bytesPoolHandler;
  /** message counter. */
  private int count = 0;
  
  /**
   * Constructor of new version MessageBytePoolPerPartition.
   */
  public MessageBytePoolPerPartition() {
  }
  
  /**
   * Constructor of new version MessageBytePoolPerPartition.
   * @param size
   */
  public MessageBytePoolPerPartition(int size) {
    initialize(size);
  }
  
  /**
   * Initialize the inner state. Must be called before {@code add()} is called.
   */
  public void initialize() {
  }
  
  /**
   * Reinitialize of inner state.
   */
  public void reInitialize() {
    this.bytesPoolHandler.reInitialize();
  }
  
  /**
   * Initialize the inner state, with a known size. Must be called before
   * {@code add()} is called.
   * @param expectedSize
   *        Number of bytes to be expected
   */
  public void initialize(int expectedSize) {
    this.bytesPoolHandler = CommunicationFactory.createBytesPool(expectedSize);
  }
  
  /**
   * Add a vertex id and data pair to the collection.
   * @param m
   *        IMessage
   */
  public void add(IMessage m) {
    try {
      m.write(bytesPoolHandler);
      count++;
    } catch (IOException e) {
      throw new RuntimeException(
          "[MessageBytePoolPerPartition] add exception: ", e);
    }
  }
  
  /**
   * Get the number of bytes used.
   * @return Bytes used
   */
  public int getSize() {
    return bytesPoolHandler.getPos();
  }
  
  /**
   * Get the size of this object in serialized form.
   * @return The size (in bytes) of the serialized object
   */
  public int getSerializedSize() {
    return 1 + 4 + getSize();
  }
  
  /**
   * Check if the list is empty.
   * @return Whether the list is empty
   */
  public boolean isEmpty() {
    return bytesPoolHandler.getPos() == 0;
  }
  
  /**
   * Clear the list.
   */
  public void clearBSPMsgs() {
    bytesPoolHandler.reset();
  }
  
  /**
   * Get the underlying byte-array.
   * @return The underlying byte-array
   */
  // Note I Need This When The Message Is Coming And To Be Placed Into The
  // Corresponding Partition.
  public byte[] getByteArray() {
    return bytesPoolHandler.getByteArray();
  }
  
  /**
   * Message Receive Is A Sequence Of MessageBytePoolPerPartition.
   * @param msgBytesPool
   */
  public void addWholeBytesPool(MessageBytePoolPerPartition msgBytesPool) {
    try {
      // Note Error Ever2014-01-22
      this.count += msgBytesPool.getMsgCount();
      bytesPoolHandler.write(msgBytesPool.getByteArray(), 0,
          msgBytesPool.getSize());
    } catch (IOException e) {
      throw new RuntimeException(
          "[MessageBytePoolPerPartition] addWholeBytesPool exception: ", e);
    }
  }
  
  /**
   * Get Iterator For Deserialization.
   * @return MessageIterator
   */
  MessageIterator getIterator() {
    return new MessageIterator(bytesPoolHandler);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(count);
    out.writeInt(bytesPoolHandler.getPos());
    out.write(bytesPoolHandler.getByteArray(), 0, bytesPoolHandler.getPos());
    // Log.info("<##########Debug1 > write Size " +bytesPoolHandler.getPos());
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    count = in.readInt();
    int size = in.readInt();
    // Note Ever Error Here.Big Bang!!!!!
    byte[] buf = new byte[size];
    in.readFully(buf);
    this.bytesPoolHandler = CommunicationFactory.createBytesPool(buf, size);
    // Note Initialize The BytesPool And The Handler.
    // Log.info("<##########Debug1 > RCount " + count + " Size: " +size
    // +" ByteSize " + getSize() );
  }
  
  @Override
  public int getMsgSize() {
    return this.getSize();
  }
  
  @Override
  public int getMsgCount() {
    // TODO Auto-generated method stub
    return count;
  }
}
