plugins {
    java
}

repositories {
    maven {
        url = uri("../build/staging-repo")
    }
    mavenCentral()
}

dependencies {
    val chronosClientVersion = providers.environmentVariable("CHRONOS_CLIENT_VERSION")
        .orElse("0.1.0-SNAPSHOT")
    implementation("io.github.rleungx:chronos-java-client:${chronosClientVersion.get()}")
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}
