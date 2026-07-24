# Build setup — Gradle & Maven (complete snippets)

StoatFlow resolves from `https://maven.stoatflow.io/releases` only. Username form `customer-<your-slug>`;
token from your onboarding email. Keep `mavenCentral()` for third-party deps.

## Gradle

**Credentials** — `~/.gradle/gradle.properties` (HOME dir, never committed):
```properties
stoatflowRepoUser=customer-your-slug
stoatflowRepoToken=PASTE_YOUR_MAVEN_TOKEN_FROM_THE_ONBOARDING_EMAIL
```

**Dependency repository** — `settings.gradle.kts`:
```kotlin
dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven {
            url = uri("https://maven.stoatflow.io/releases")
            credentials {
                username = providers.gradleProperty("stoatflowRepoUser").get()
                password = providers.gradleProperty("stoatflowRepoToken").get()
            }
        }
    }
}
```

**Plugin repository** — also in `settings.gradle.kts` (needed to resolve the `io.stoatflow` plugin marker):
```kotlin
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven {
            url = uri("https://maven.stoatflow.io/releases")
            credentials {
                username = providers.gradleProperty("stoatflowRepoUser").get()
                password = providers.gradleProperty("stoatflowRepoToken").get()
            }
        }
    }
}
```

**Apply the plugin + deps** — `build.gradle.kts`:
```kotlin
plugins {
    kotlin("jvm") version "2.4.0"          // omit for a Java-only project
    id("io.stoatflow") version "<stoatflow-version>"
}
stoatflow {
    mainClass.set("com.example.MainKt")
    // docker { enabled.set(true); imageName.set("my-app") }        // see native-and-docker.md
    // nativeImage { enabled.set(true); gc.set("G1") }
}
dependencies {
    implementation("io.stoatflow:stoatflow-runtime:<stoatflow-version>")
    // implementation("io.stoatflow:stoatflow-core:<stoatflow-version>")   // DSL/engine only
    testImplementation("io.stoatflow:stoatflow-test-utils:<stoatflow-version>")
}
```

The plugin unconditionally applies: `application`, `com.gradleup.shadow` (fat JAR classifier `all`), the
JDK-25 toolchain, Kotlin `jvmToolchain(25)` (only if the Kotlin plugin is present — it is **not** bundled),
`--enable-preview` on compile, and the `integrationTest` task (JUnit-Platform, tag `IntegrationTest`).

**Without the plugin** (configure JVM flags yourself):
```kotlin
java { toolchain { languageVersion = JavaLanguageVersion.of(25) } }
tasks.withType<JavaCompile> { options.compilerArgs.add("--enable-preview") }
application {
    applicationDefaultJvmArgs = listOf(
        "--enable-preview", "--enable-native-access=ALL-UNNAMED", "-XX:+UseG1GC")
}
```

## Maven

Three artifacts: `io.stoatflow:stoatflow-bom` (dependencyManagement), `io.stoatflow:stoatflow-parent`
(convention parent POM; imports the BOM), `io.stoatflow:stoatflow-maven-plugin` (four goals, prefix
`stoatflow`).

**Credentials** — `~/.m2/settings.xml`:
```xml
<settings>
  <servers>
    <server>
      <id>stoatflow-releases</id>
      <username>customer-your-slug</username>
      <password>PASTE_YOUR_MAVEN_TOKEN_FROM_THE_ONBOARDING_EMAIL</password>
    </server>
  </servers>
  <pluginGroups>
    <pluginGroup>io.stoatflow</pluginGroup>   <!-- enables the short `mvn stoatflow:<goal>` prefix -->
  </pluginGroups>
</settings>
```

**Parent + repo + deps** — `pom.xml`:
```xml
<parent>
  <groupId>io.stoatflow</groupId>
  <artifactId>stoatflow-parent</artifactId>
  <version>REPLACE-WITH-CURRENT-RELEASE</version>   <!-- literal — Maven can't resolve a property in <parent> -->
  <relativePath/>
</parent>

<properties>
  <stoatflow.mainClass>com.example.Main</stoatflow.mainClass>
</properties>

<repositories>
  <repository>
    <id>stoatflow-releases</id>                       <!-- MUST match the settings.xml <server><id> -->
    <url>https://maven.stoatflow.io/releases</url>
    <releases><enabled>true</enabled></releases>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>io.stoatflow</groupId>
    <artifactId>stoatflow-runtime</artifactId>        <!-- version managed by the BOM the parent imports -->
  </dependency>
  <dependency>
    <groupId>io.stoatflow</groupId>
    <artifactId>stoatflow-test-utils</artifactId>
    <scope>test</scope>
  </dependency>
</dependencies>
```

**Gotchas:** the `<repository><id>` must equal the `settings.xml` `<server><id>` (`stoatflow-releases`) —
that's how Maven applies your credentials. Profiles activate with `-P` (e.g. `-Pstoatflow-docker`), **not**
`<properties>`. Config knobs live in `<properties>` (`stoatflow.mainClass`, `stoatflow.docker.*`,
`stoatflow.nativeImage.*`). The four Mojo goals: `stoatflow:configure`, `stoatflow:copy-entrypoint`,
`stoatflow:native-docker-build`, `stoatflow:run`.

**Corporate-parent path (BOM only, no StoatFlow parent):** import `stoatflow-bom` (`<type>pom</type>
<scope>import</scope>`) in `<dependencyManagement>` with an explicit `${stoatflow.version}` property, and
wire the compiler `--enable-preview` + JVM run flags yourself.
