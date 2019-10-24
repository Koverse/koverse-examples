Koverse Analytics Examples

Analytics examples are divided up by language: Scala, Java, and Python.

JVM based examples

To run these examples, build the project using maven 'mvn package', then upload to Koverse using the UI. After that you can run the functional examples.

Steps
- Run Koverse 3.0.3 or later
- Go to Koverse UI and login (see Koverse documentation for details on running the docker developer image at https://koverse.readthedocs.io/en/3.0/dev/dev_image.html )
- 'cd analytics/scala/' or 'cd analytics/java'
- 'mvn package'

You can now upload the transform as an Addon in the UI under Admin. See https://koverse.readthedocs.io/en/3.0/dev/addons.html#uploading-an-addon-to-koverse for instructions on Uploading an Addon.

Now you can configure and run the transform from the UI. Import data (https://koverse.readthedocs.io/en/3.0/usage/import.html). Then configure the transform (https://koverse.readthedocs.io/en/3.0/usage/transforms.html)
