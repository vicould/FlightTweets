Flightsdb
=========

Import the project in eclipse. Then to use the application, you need to first start the database, executing the following command from the root (where this README is located):

    java -cp lib/hsqldb.jar org.hsqldb.server.Server --database.0 file:flightsdb --dbname.0 flightsdb

This allows you to start the server, using the database contained in the root ofthe project.
If you want, there is the possibility to access the database using a GUI manager. You can launch it using the hsqldb archive:

    java -cp lib/hsqldb.jar org.hsqldb.util.DatabaseManagerSwing

To connect to the db, you have to fill the fields using the following values:

    Type: HSQL Database Engine Server
    Driver: org.hsqldb.jdbcDriver
    URL: jdbc:hsqldb:hsql://localhost/flightsdb
    User: SA
    Password: (no password)

There you can have access to all the database, perform some queries.

Then, to use the real application, in eclipse using the Run/Run Configurations menu, create a new configuration for the Launcher class. Specify as argument input_ids.txt, which is the file containing the usernames to fetch.

