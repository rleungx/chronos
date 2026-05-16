plugins {
    id("java")
    id("com.google.protobuf") version "0.10.0"
    id("maven-publish")
}

repositories {
    mavenCentral()
}

group = "io.github.rleungx"
version = "0.1.0"

val grpcVersion = "1.76.0"
val protobufVersion = "4.34.1"

dependencies {
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.3")
}

sourceSets {
    main {
        java {
            srcDir("src/main/java")
        }
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
                licenses {
                    license {
                        name.set("MIT OR Apache-2.0")
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
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
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
