plugins {
    id("java")
}

allprojects {
    group = "me.xiaoying.moebroker"
    version = "1.0.0"

    repositories {
        mavenCentral()

        maven("https://312Hz.github.io/maven-repository")
    }
}

subprojects {
    apply(plugin = "java")

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    dependencies {
        // logger
        implementation("me.xiaoying:logger:1.0.1")

        // lombok
        compileOnly("org.projectlombok:lombok:1.18.30")
        annotationProcessor("org.projectlombok:lombok:1.18.30")
        testCompileOnly("org.projectlombok:lombok:1.18.30")
        testAnnotationProcessor("org.projectlombok:lombok:1.18.30")
    }
}