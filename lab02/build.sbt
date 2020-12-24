lazy val commonSettings = Seq(
  name := "Lab02",
  version := "1.0",
  scalaVersion := "2.11.12",
  libraryDependencies ++= Seq(
    "org.apache.spark" %%  "spark-core" % "2.4.6",
    "org.apache.spark" %%  "spark-sql" % "2.4.6"
  )

)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "lab02_2.11-1.0.jar"