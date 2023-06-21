import co.bird.Library

dependencies {
  api(project(":module-api"))

  api(Library.Jdbi.kotlin)
  api(Library.Database.postgisJdbc)

  implementation(Library.Kotlin.reflect)

  implementation(Library.Jackson.core)
  implementation(Library.Jackson.kotlin)
  implementation(Library.Jackson.databind)

  implementation(Library.ApacheCommon.lang3)

  implementation(Library.Database.hikari)

  testImplementation(Library.Testing.jupiter)
  testImplementation(Library.Testing.assertj)
  testImplementation(Library.Testing.mockk)
}

tasks.withType<Test> {
  useJUnitPlatform()
}
