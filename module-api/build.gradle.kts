import co.bird.Library

dependencies {
  implementation(Library.Kotlin.reflect)
}

tasks.withType<Test> {
  useJUnitPlatform()
}
