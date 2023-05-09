package org.plexq.aws.dynamo

sealed abstract class Done extends Serializable

case object Done extends Done {

  /**
   * Java API: the singleton instance
   */
  def getInstance(): Done = this

  /**
   * Java API: the singleton instance
   *
   * This is equivalent to [[Done.getInstance]], but can be used with static import.
   */
  def done(): Done = this
}

