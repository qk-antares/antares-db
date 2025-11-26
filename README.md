```shell
mvn compile

mvn exec:java -Dexec.mainClass="com.antares.db.backend.Launcher" -Dexec.args="-create ~/workplace/java/antares-db/mydb"

mvn exec:java -Dexec.mainClass="com.antares.db.client.Launcher" -Dexec.args="-create ~/workplace/java/antares-db/mydb"
```