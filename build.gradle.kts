// Apply the java plugin to add support for Java
plugins {
    java
    application
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    jcenter()
}

dependencies {
    // Our beloved one-nio
    compile("ru.odnoklassniki:one-nio:1.2.0")

    // Logging
    compile("org.slf4j:slf4j-api:1.7.26")
    compile("ch.qos.logback:logback-classic:1.2.3")

    // Annotations for better code documentation
    compile("com.intellij:annotations:12.0")

    // Guava primitives
    compile("com.google.guava:guava:27.0.1-jre")

    // JUnit Jupiter test framework
    testCompile("org.junit.jupiter:junit-jupiter-api:5.4.0")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:5.4.0")

    // https://mvnrepository.com/artifact/org.rocksdb/rocksdbjni
    compile("org.rocksdb", "rocksdbjni", "6.2.2")
}

val run by tasks.getting(JavaExec::class) {
    standardInput = System.`in`
}

tasks {
    test {
        maxHeapSize = "128m"
        useJUnitPlatform()
    }
}

application {
    // Define the main class for the application
    mainClassName = "ru.mail.polis.Server"

    // And limit Xmx
    applicationDefaultJvmArgs = listOf("-Xmx128m")
}
