package co.bird

object Library {
  // Kotlin
  object Kotlin {
    const val stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Version.kotlin}"
    const val reflect = "org.jetbrains.kotlin:kotlin-reflect:${Version.kotlin}"
  }

  // Jdbi
  object Jdbi {
    const val core = "org.jdbi:jdbi3-core:${Version.jdbi}"
    const val kotlin = "org.jdbi:jdbi3-kotlin:${Version.jdbi}"
    const val sqlObject = "org.jdbi:jdbi3-kotlin-sqlobject:${Version.jdbi}"
    const val postgres = "org.jdbi:jdbi3-postgres:${Version.jdbi}"
  }

  object Database {
    const val postgresql = "org.postgresql:postgresql:${Version.postgresql}"
    const val postgisJdbc = "net.postgis:postgis-jdbc:${Version.postgis}"
    const val hikari = "com.zaxxer:HikariCP:${Version.hikari}"
  }

  object Jackson {
    const val api = "com.fasterxml.jackson.core:jackson-annotations:${Version.jackson}"
    const val core = "com.fasterxml.jackson.core:jackson-core:${Version.jackson}"
    const val databind = "com.fasterxml.jackson.core:jackson-databind:${Version.jackson}"
    const val kotlin = "com.fasterxml.jackson.module:jackson-module-kotlin:${Version.jackson}"
  }

  object ApacheCommon {
    const val lang3 = "org.apache.commons:commons-lang3:3.9"
  }

  object Testing {
    const val jupiter = "org.junit.jupiter:junit-jupiter:${Version.junit5}"
    const val jupiterEngine = "org.junit.jupiter:junit-engine:${Version.junit5}"
    const val junit5Api = "org.junit.jupiter:junit-jupiter-api:${Version.junit5}"
    const val junit5VintageEngine = "org.junit.vintage:junit-vintage-engine:${Version.junit5}"
    const val assertj = "org.assertj:assertj-core:3.23.1"
    const val mockk = "io.mockk:mockk:1.12.4"
  }
}
