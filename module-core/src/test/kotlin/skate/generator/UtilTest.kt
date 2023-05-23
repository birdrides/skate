package skate.generator

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class UtilTest {
  @Test
  fun toUnderscore() {
    assertThat("SomeThing".toUnderscore()).isEqualTo("some_thing")
    assertThat("a_b".toUnderscore()).isEqualTo("a_b")
    assertThat("A_B".toUnderscore()).isEqualTo("a_b")
    assertThat("AA".toUnderscore()).isEqualTo("aa")
    assertThat("BB".toUnderscore()).isEqualTo("bb")
    assertThat("".toUnderscore()).isEqualTo("")
    assertThat("._.".toUnderscore()).isEqualTo("._.")
    assertThat("_".toUnderscore()).isEqualTo("_")
    assertThat("A_longMismatchSentence_likeThis".toUnderscore()).isEqualTo("a_long_mismatch_sentence_like_this")
  }
}
