
package com.chinamobile.bcbsp.rpc;

import com.chinamobile.bcbsp.comm.BSPMessagesPack;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.comm.io.util.WritableBSPMessages;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 *
 * RPCCommunicationProtocol  A interface that extends VersionedProtocol which
 * Superclass of all protocols that use Hadoop RPC.
 */
public interface RPCCommunicationProtocol extends VersionedProtocol {
  /** State protocolVersion */
  Long protocolVersion = 1L;

  /**
   * In order to support the RPC,
   * MSG support serialization is required. The writable interface inheritance
   * @param packedMessages
   *        BSPMessagesPack
   * @return
   *        receive PackedMessage
   */
  int sendPackedMessage(BSPMessagesPack packedMessages);

  /**
   * for test
   *
   * @param packedMessages
   *        BSPMessagesPack
   * @param str
   *        String
   * @return
   *        0
   */
  int sendPackedMessage(BSPMessagesPack packedMessages, String str);

  /**
   *  Note Add 20140310 for new Communication Version.
   * @param messages
   *        IMessage
   * @return
   *        0
   */
  int sendUnpackedMessage(IMessage messages);

  /**
   * send PackedMessage
   * @param packedMessages
   *        WritableBSPMessages
   * @param srcPartition
   *         int
   * @param dstBucket
   *        int
   * @param superstep
   *        int
   * @return
   *        0
   */
  int sendPackedMessage(WritableBSPMessages packedMessages,
      int srcPartition, int dstBucket, int superstep);
}
