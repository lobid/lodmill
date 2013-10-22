import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

    val appName         = "lodmill-ui"
    val appVersion      = "0.1.0-SNAPSHOT"
    
    val appDependencies = Seq(
      javaCore,
      "org.elasticsearch" % "elasticsearch" % "0.90.5" withSources(),
      "org.lobid" % "lodmill-ld" % "0.2.1-SNAPSHOT",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test"
    )

    val main = play.Project(appName, appVersion, appDependencies).settings(
      parallelExecution in Test := false,
      resolvers := Seq("codehaus" at "http://repository.codehaus.org/org/codehaus", "typesafe" at "http://repo.typesafe.com/typesafe/repo", Resolver.mavenLocal)
    )
	
}
