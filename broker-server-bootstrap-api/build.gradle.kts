plugins {
    id("moebroker-publish")
}

dependencies {
    compileOnly(project(":broker-api"))
    compileOnly(project(":broker-server"))

    compileOnly(libs.snakeyaml)
}