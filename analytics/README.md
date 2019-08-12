Koverse Analytics Examples

Analytics examples are divided up by language: Scala, Java, and Python.

JVM based examples

The Java and Scala examples include code to run the examples in Koverse. This code is located in the test directory but doesn't run at build time. To run, build the project using maven 'mvn package', then upload to Koverse using the UI. After that you can run the functional examples.

Steps
- Run Koverse 3.0.3 or later
- Go to Koverse UI and create an API token and API client (see Koverse documentation for details)
- Copy client secret
- Update client.properties in src/test/resources with client name and client secret
- 'cd analytics/scala/'
- 'mvn package'
- 'mvn -Dtest=TESTSUITE test'. Example testsuite is ScalaAnalyticsExamplesFT
- You should see output as the transforms are created, run, and sample records are output at the end.

