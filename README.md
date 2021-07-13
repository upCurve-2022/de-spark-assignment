# **Spark Assignments**

1. Clone this repository to your local.
2. Create a scala object with the main function under src/main/scala/sparkcodes/weekone
3. Name the scala object with your name.
4. A sample template with questions and instructions is already created.
5. Answer all the questions and push your code to git.
6. NOTE: Remember to add spark dependencies, build it again and make sure there are no build errors
7. Check if you are able to import **org.apache.spark** package in the code.

Exercise status: To be done

### **Dependencies to add in the build.sbt file**

```
name := "some-name"
version := "1.0.0"
scalaVersion := "2.11.8"
val sparkVersion = "2.3.2"
val jacksonCore = "2.6.7"
val scope = "compile"

libraryDependencies ++= Seq(
  "org.scalamock" %% "scalamock" % "4.4.0" % Test,
  //dependency of reading configuration
  "com.typesafe" % "config" % "1.3.3",
  //spark core libraries, in the production or for spark-submit in local add provided so that dependent jar is not part of assembly jar
  // ex :"org.apache.spark" %% "spark-core" % sparkVersion % provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % scope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % scope,
  "org.apache.spark" %% "spark-hive" % sparkVersion % scope,
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonCore % scope,
  //logging library
  "org.slf4j" % "slf4j-api" % "1.7.29",
  //for doing testing
  "org.scalatest" %% "scalatest" % "3.1.0" % Test
)
```
