package bio.ferlab.datalake.spark3.loader

import java.sql.Timestamp

case class TestData(uid: String,
                    oid: String,
                    createdOn: Timestamp,
                    updatedOn: Timestamp,
                    data: Long,
                    chromosome: String = "1",
                    start: Long = 666)
