
package com.chinamobile.bcbsp.examples.semicluster;

import java.util.Comparator;

/**
 * Comparator that sorts semi-clusters according to their score.
 */
public class ClusterScoreComparator implements Comparator<SemiCluster> {
  /**
   * Compare two semi-clusters for order.
   * @param o1
   *        the first object
   * @param o2
   *        the second object
   * @return -1 if score for object1 is smaller than score of object2, 1 if score
   *         for object2 is smaller than score of object1 or 0 if both scores
   *         are the same
   */
  @Override
  public int compare(final SemiCluster o1, final SemiCluster o2) {
    if (o1.getScore() < o2.getScore()) {
      return -1;
    } else if (o1.getScore() > o2.getScore()) {
      return 1;
    } else {
      // We add this for consistency with the equals() method.
      if (!o1.equals(o2)) {
        return 1;
      }
    }
    return 0;
  }
}
