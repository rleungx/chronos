plugins {
    id("java")
    id("com.google.protobuf") version "0.9.4"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.google.protobuf:protobuf-java:4.28.3")
    implementation("io.grpc:grpc-okhttp:1.68.1")
    implementation("io.grpc:grpc-protobuf:1.68.1")
    implementation("io.grpc:grpc-stub:1.68.1")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")
    testImplementation("io.grpc:grpc-inprocess:1.68.1")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.3")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

sourceSets {
    main {
        java {
            srcDir("src/main/java")
            srcDir("../../examples/java")
        }
        resources {
            setSrcDirs(emptyList<String>())
        }
        proto {
            srcDir("proto")
            include("tso.proto")
        }
    }
    test {
        resources {
            setSrcDirs(emptyList<String>())
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.28.3:osx-aarch_64@exe"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.68.1:osx-aarch_64@exe"
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
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.test {
    useJUnitPlatform()
}

tasks.named("generateProto") {
    mustRunAfter(tasks.named("processResources"))
    mustRunAfter(tasks.named("processTestResources"))
    mustRunAfter(tasks.named("extractIncludeTestProto"))
    mustRunAfter(tasks.named("extractTestProto"))
}

tasks.named("generateTestProto") {
    enabled = false
}
