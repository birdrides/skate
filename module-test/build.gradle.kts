import co.bird.Library

dependencies {
  implementation(project(":module-api"))
  implementation(project(":module-core"))

  testImplementation(Library.Jackson.databind)
  testImplementation(Library.Jackson.core)
  testImplementation(Library.Jackson.kotlin)

  testImplementation(Library.Testing.jupiter)
  testImplementation(Library.Testing.assertj)
  testImplementation(Library.Database.postgresql)
}

tasks.withType<Test> {
  useJUnitPlatform()
}
