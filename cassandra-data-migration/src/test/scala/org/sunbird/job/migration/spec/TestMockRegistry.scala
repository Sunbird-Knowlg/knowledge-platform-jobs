package org.sunbird.job.migration.spec

import org.sunbird.job.util.CassandraUtil

object TestMockRegistry extends Serializable {
  var cassandraUtil: CassandraUtil = _
}
