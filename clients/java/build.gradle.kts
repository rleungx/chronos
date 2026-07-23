import org.gradle.external.javadoc.StandardJavadocDocletOptions

plugins {
    id("java-library")
    id("com.google.protobuf") version "0.10.0"
    id("maven-publish")
}

repositories {
    mavenCentral()
}

group = "io.github.rleungx"
version = providers.environmentVariable("CHRONOS_CLIENT_VERSION").orElse("0.1.0-SNAPSHOT").get()

java {
    withSourcesJar()
    withJavadocJar()
}

val grpcVersion = "1.76.0"
val protobufVersion = "4.34.1"

dependencies {
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    api("com.google.protobuf:protobuf-java:$protobufVersion")
    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("io.grpc:grpc-stub:$grpcVersion")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.3")
}

sourceSets {
    main {
        resources {
            setSrcDirs(emptyList<String>())
        }
        proto {
            srcDir(layout.buildDirectory.dir("clientProto"))
            include("tso.proto")
        }
    }
    test {
        resources {
            setSrcDirs(emptyList<String>())
        }
    }
}

val exampleSourceSet = sourceSets.create("example") {
    java {
        srcDir("../../examples/java")
    }
    resources {
        setSrcDirs(emptyList<String>())
    }
    compileClasspath += sourceSets["main"].output + configurations["runtimeClasspath"]
    runtimeClasspath += output + compileClasspath
}

val syncRootProto by tasks.registering(Copy::class) {
    from(layout.projectDirectory.file("../../tso.proto"))
    into(layout.buildDirectory.dir("clientProto"))
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

tasks.register<JavaExec>("runExample") {
    group = "application"
    mainClass.set("ClientExample")
    classpath = exampleSourceSet.runtimeClasspath
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("Chronos Java Client")
                description.set("Application-facing Chronos timestamp client")
                url.set("https://github.com/rleungx/chronos")
                scm {
                    connection.set("scm:git:https://github.com/rleungx/chronos.git")
                    developerConnection.set("scm:git:ssh://git@github.com/rleungx/chronos.git")
                    url.set("https://github.com/rleungx/chronos")
                }
                licenses {
                    license {
                        name.set("Apache-2.0")
                    }
                }
            }
        }
    }
    repositories {
        maven {
            name = "localStaging"
            url = layout.buildDirectory.dir("staging-repo").get().asFile.toURI()
        }
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/rleungx/chronos")
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: ""
                password = System.getenv("GITHUB_TOKEN") ?: ""
            }
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}

tasks.withType<Javadoc>().configureEach {
    exclude("com/chronos/tso/v1/**")
    (options as StandardJavadocDocletOptions).addBooleanOption("Werror", true)
}

tasks.named("generateProto") {
    dependsOn(syncRootProto)
    mustRunAfter(tasks.named("processResources"))
    mustRunAfter(tasks.named("processTestResources"))
    mustRunAfter(tasks.named("extractIncludeTestProto"))
    mustRunAfter(tasks.named("extractTestProto"))
}

tasks.named("generateTestProto") {
    enabled = false
}

tasks.named("processResources") {
    mustRunAfter(syncRootProto)
}
