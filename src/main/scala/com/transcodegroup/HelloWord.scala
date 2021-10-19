package com.transcodegroup

import lombok.Data

@Data
object HelloWord {
  def main(args: Array[String]): Unit = {
    print("hello word")
  }
}
