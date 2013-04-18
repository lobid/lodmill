import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

    val appName         = "lodmill-ui"
    val appVersion      = "0.1.0-SNAPSHOT"
    
    val appDependencies = Seq(
      javaCore,
      "org.elasticsearch" % "elasticsearch" % "0.19.11" withSources(),
      "org.lobid" % "lodmill-ld" % "0.1.0-SNAPSHOT"
    )

    val main = play.Project(appName, appVersion, appDependencies).settings(
      resolvers := Seq("codehaus" at "http://repository.codehaus.org/org/codehaus", "typesafe" at "http://repo.typesafe.com/typesafe/repo", Resolver.mavenLocal)
    )
	
}
