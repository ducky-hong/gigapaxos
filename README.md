# gigapaxos

Refer to the Getting Started page in the
[Wiki](<https://github.com/MobilityFirst/gigapaxos/wiki>) above for tutorials.

## Gradle

__Build a jar__

```bash
$ ./gradlew clean assemble shadowJar
$ ls build/libs/gigapaxos-<version>-all.jar
```

__Run a reconfigurator or an app container__

```bash
$ java -cp build/libs/gigapaxos-<version>-all.jar edu.umass.cs.reconfiguration.ReconfigurableNode <server name>
```