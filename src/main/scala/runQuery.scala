import java.sql.DriverManager
import java.sql.Connection
import scala.io._


object runQuery {

  def main(args : Array[String] ) {
    val sqlFile = "H:/vertiv/query.txt"
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
}