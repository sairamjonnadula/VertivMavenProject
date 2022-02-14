import org.apache.spark.sql.SparkSession

import java.sql.{Connection, DriverManager}
import scala.io.Source

object runQueryOnSpark extends App {

  val spark = SparkSession.builder().master("local[1]").appName("RunQueryFromFile").getOrCreate();

    val sqlFile = "H:/vertiv/query2.txt"
    val driver = "oracle.jdbc.driver.OracleDriver"
    val url = "jdbc:oracle:thin:@localhost:1521/xe"
    val username = "SYS as SYSDBA"
    val password = "admin"

  var connection: Connection = null


  try {
    // make the connection
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
  } catch {
    case e: Throwable => e.printStackTrace
      println("Connection NOT Established...")
  }

  var statement = connection.createStatement()

  //var resultSet = statement.executeQuery("INSERT INTO ANONYMOUS.dum1 SELECT * FROM ANONYMOUS.dum")


  Source.fromFile(sqlFile).mkString.replaceAll("\r\n", "").split(";").map(x => {
    println("------Executing...-----------")
    println(x)
    println("-----------------------------")
    statement.executeQuery(x)
  }

  )

  connection.close()


}
