import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

    val appName         = "lodmill-ui"
    val appVersion      = com.typesafe.config.ConfigFactory.parseFile(new File("conf/application.conf")).resolve().getString("application.version")
    
    val appDependencies = Seq(
      javaCore,
      "org.elasticsearch" % "elasticsearch" % "0.90.7" withSources(),
      "org.lobid" % "lodmill-ld" % "1.5.0",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test"
    )

    val nwbib = play.Project("nwbib", path = file("modules/nwbib"))
    val oer = play.Project("oer-api", path = file("modules/oerworldmap/oer-api"))

    val main = play.Project(appName, appVersion, appDependencies).settings(
      parallelExecution in Test := false,
      resolvers := Seq(
          "codehaus" at "http://repository.codehaus.org/org/codehaus", 
          "typesafe" at "http://repo.typesafe.com/typesafe/repo", 
          "jena-dev" at "https://repository.apache.org/content/repositories/snapshots",
          Resolver.mavenLocal)
    ).dependsOn(nwbib, oer).aggregate(nwbib, oer)

}
