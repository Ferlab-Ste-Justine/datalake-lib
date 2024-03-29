package bio.ferlab.datalake.spark3.etl

import java.sql.Timestamp

case class AirportOutput(airport_id: Long = 1,
                         airport_cd: String = "YYC",
                         description_EN: String = "Calgary Int airport",
                         hash_id: String = "356a192b7913b04c54574d18c28d46e6395428ab",
                         input_file_name: String = "/path/file1.csv",
                         createdOn: Timestamp = Timestamp.valueOf("2021-03-02 00:00:00"))
